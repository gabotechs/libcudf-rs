use crate::errors::cudf_to_df;
use crate::execution::execute_cudf;
use crate::expr::ast::{is_join_filter_supported_by_cudf_ast, join_filter_to_cudf_ast};
use crate::metrics::CuDFBaselineMetrics;
use crate::physical::cudf_load::cudf_schema_compatibility_map;
use arrow::array::RecordBatch;
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::common::{JoinType, NullEquality, Statistics};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion_physical_plan::expressions::Column;
use datafusion_physical_plan::joins::utils::JoinFilter;
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion_physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, MetricsSet, Time,
};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    execute_stream, project_schema, DisplayAs, DisplayFormatType, ExecutionPlan,
    ExecutionPlanProperties, PhysicalExpr, PlanProperties,
};
use futures::{StreamExt, TryStreamExt};
use libcudf_rs::{
    CuDFAstExpression, CuDFHashJoin, CuDFNullEquality, CuDFStreamingJoin, CuDFTable, CuDFTableView,
};
use std::any::Any;
use std::fmt::Formatter;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::OnceCell;

type SharedTableFuture =
    Pin<Box<dyn Future<Output = Result<Arc<CuDFTable>, DataFusionError>> + Send>>;

/// GPU-accelerated hash join execution node.
///
/// Replaces DataFusion's `HashJoinExec` for equi-joins where all keys are
/// simple column references. Supports `Inner`, `Left`, and `Full` join types.
/// Both children are expected to be GPU-resident (via `CuDFLoadExec`).
pub struct CuDFHashJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
    filter: Option<JoinFilter>,
    join_type: JoinType,
    projection: Option<Vec<usize>>,
    partition_mode: PartitionMode,
    left_on: Vec<usize>,
    right_on: Vec<usize>,
    properties: Arc<PlanProperties>,
    shared_table: Arc<OnceCell<Arc<CuDFTable>>>,
    metrics: ExecutionPlanMetricsSet,
}

impl std::fmt::Debug for CuDFHashJoinExec {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("CuDFHashJoinExec")
            .field("join_type", &self.join_type)
            .field("partition_mode", &self.partition_mode)
            .finish()
    }
}

/// Merge left and right schemas into raw join output schema (pre-projection),
/// adjusting field nullability to match the join type, then normalize types for cuDF.
fn build_join_schema(left: &SchemaRef, right: &SchemaRef, join_type: JoinType) -> SchemaRef {
    let left_nullable = matches!(join_type, JoinType::Full);
    let right_nullable = matches!(join_type, JoinType::Left | JoinType::Full);

    let fields: Vec<_> = left
        .fields()
        .iter()
        .map(|f| {
            if left_nullable && !f.is_nullable() {
                Arc::new(Field::new(f.name(), f.data_type().clone(), true))
            } else {
                Arc::clone(f)
            }
        })
        .chain(right.fields().iter().map(|f| {
            if right_nullable && !f.is_nullable() {
                Arc::new(Field::new(f.name(), f.data_type().clone(), true))
            } else {
                Arc::clone(f)
            }
        }))
        .collect();

    cudf_schema_compatibility_map(Arc::new(Schema::new(fields)))
}

/// `(left_key, right_key)` pairs used to express equi-join conditions.
type JoinOnExprs = [(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)];

fn extract_column_indices(
    on: &JoinOnExprs,
    left_side: bool,
) -> Result<Vec<usize>, DataFusionError> {
    on.iter()
        .map(|(l, r)| {
            let expr = if left_side { l } else { r };
            expr.as_any()
                .downcast_ref::<Column>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "CuDFHashJoinExec: join key is not a Column expression".into(),
                    )
                })
                .map(|c| c.index())
        })
        .collect()
}

impl CuDFHashJoinExec {
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
        filter: Option<JoinFilter>,
        join_type: JoinType,
        projection: Option<Vec<usize>>,
        partition_mode: PartitionMode,
    ) -> Result<Self, DataFusionError> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        let join_schema = build_join_schema(&left_schema, &right_schema, join_type);
        let output_schema = project_schema(&join_schema, projection.as_ref())?;
        let left_on = extract_column_indices(&on, true)?;
        let right_on = extract_column_indices(&on, false)?;
        let right_partition_count = right.output_partitioning().partition_count();

        let left_len = left_schema.fields().len();
        let right_len = right_schema.fields().len();
        for (l, r) in left_on.iter().zip(&right_on) {
            if *l >= left_len || *r >= right_len {
                return datafusion::common::plan_err!(
                    "CuDFHashJoinExec: on-key index out of bounds (left={l}/{left_len}, right={r}/{right_len})"
                );
            }
        }

        if !supports_streaming_join(&partition_mode, join_type, right_partition_count) {
            return datafusion::common::plan_err!(
                "CuDFHashJoinExec: unsupported join mode {:?} with join type {:?}",
                partition_mode,
                join_type
            );
        }

        // Output partitioning follows the probe side for CollectLeft, and the build side
        // otherwise.
        let output_partitioning = match partition_mode {
            PartitionMode::CollectLeft => right.output_partitioning().clone(),
            _ => left.output_partitioning().clone(),
        };
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            output_partitioning,
            left.pipeline_behavior(),
            left.boundedness(),
        ));

        Ok(Self {
            left,
            right,
            on,
            filter,
            join_type,
            projection,
            partition_mode,
            left_on,
            right_on,
            properties,
            shared_table: Arc::new(OnceCell::new()),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Extract fields from a DataFusion `HashJoinExec` and call `try_new`.
    pub fn from_hash_join_exec(node: &HashJoinExec) -> Result<Self, DataFusionError> {
        Self::try_new(
            node.left().clone(),
            node.right().clone(),
            node.on().to_vec(),
            node.filter().cloned(),
            *node.join_type(),
            node.projection.as_ref().map(|p| p.to_vec()),
            *node.partition_mode(),
        )
    }
}

impl DisplayAs for CuDFHashJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let on_keys: Vec<String> = self.on.iter().map(|(l, r)| format!("{l} = {r}")).collect();
        write!(
            f,
            "CuDFHashJoinExec: mode={:?}, join_type={:?}, on=[{}]",
            self.partition_mode,
            self.join_type,
            on_keys.join(", ")
        )?;
        if let Some(filter) = &self.filter {
            write!(f, ", filter={}", filter.expression())?;
        }
        Ok(())
    }
}

impl ExecutionPlan for CuDFHashJoinExec {
    fn name(&self) -> &str {
        "CuDFHashJoinExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn partition_statistics(
        &self,
        _partition: Option<usize>,
    ) -> Result<Statistics, DataFusionError> {
        Ok(Statistics::new_unknown(&self.schema()))
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let right = children.swap_remove(1);
        let left = children.swap_remove(0);
        Ok(Arc::new(Self::try_new(
            left,
            right,
            self.on.clone(),
            self.filter.clone(),
            self.join_type,
            self.projection.clone(),
            self.partition_mode,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let right_stream = self.right.execute(partition, Arc::clone(&context))?;
        let metrics = CuDFHashJoinMetrics::new(&self.metrics, partition);

        // CollectLeft: all partition streams share one left table via OnceCell,
        // so the left child is executed at most once regardless of partition count.
        // Partitioned: each partition builds its own left table independently.
        let left_fut = match &self.partition_mode {
            PartitionMode::CollectLeft => collect_shared(
                Arc::clone(&self.shared_table),
                Arc::clone(&self.left),
                Arc::clone(&context),
                metrics.clone(),
            ),
            _ => {
                let left_stream = self.left.execute(partition, Arc::clone(&context))?;
                let metrics = metrics.clone();
                Box::pin(async move {
                    let _timer = metrics.build_time.timer();
                    let batches: Vec<RecordBatch> = left_stream.try_collect().await?;
                    metrics.record_build_input(&batches);
                    batches_to_table(&batches).map(Arc::new)
                })
            }
        };

        let join_type = self.join_type;
        let left_on = self.left_on.clone();
        let right_on = self.right_on.clone();
        let filter = self.filter.clone();
        let projection = self.projection.clone();
        let output_schema = self.schema();
        let right_schema = self.right.schema();

        let plan = JoinPlan {
            join_type,
            left_on,
            right_on,
            filter,
            output_schema: output_schema.clone(),
            right_schema,
            projection,
        };

        debug_assert!(supports_streaming_join(
            &self.partition_mode,
            plan.join_type,
            self.right.output_partitioning().partition_count()
        ));

        let stream = futures::stream::try_unfold(
            StreamingJoinState::new(plan, left_fut, right_stream, metrics),
            StreamingJoinState::next_batch,
        );

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            stream,
        )))
    }
}

/// Materialize the left side once and share across partitions via `OnceCell`.
fn collect_shared(
    shared: Arc<OnceCell<Arc<CuDFTable>>>,
    left_child: Arc<dyn ExecutionPlan>,
    ctx: Arc<TaskContext>,
    metrics: CuDFHashJoinMetrics,
) -> SharedTableFuture {
    Box::pin(async move {
        shared
            .get_or_try_init(|| async move {
                let _timer = metrics.build_time.timer();
                let stream = execute_stream(left_child, ctx).map_err(Arc::new)?;
                let batches: Vec<RecordBatch> = stream.try_collect().await.map_err(Arc::new)?;
                metrics.record_build_input(&batches);
                batches_to_table(&batches).map(Arc::new).map_err(Arc::new)
            })
            .await
            .map(Arc::clone)
            .map_err(|e: Arc<DataFusionError>| DataFusionError::External(Box::new(e)))
    })
}

/// Join shapes that can stream right-side batches through a reusable build-side hash table.
///
/// `CollectLeft` streams against the one shared left/build table. `Partitioned`
/// streams per partition, where each partition has its own left/build table.
/// `Auto` is rejected until this wrapper knows which distribution DataFusion
/// selected for the physical join.
///
/// TODO: Support `CollectLeft` `Left`/`Full` joins with multiple right
/// partitions by probing all right partitions through one GPU join state and
/// finalizing unmatched build rows once.
fn supports_streaming_join(
    partition_mode: &PartitionMode,
    join_type: JoinType,
    right_partition_count: usize,
) -> bool {
    match (partition_mode, join_type) {
        (PartitionMode::CollectLeft, JoinType::Inner) => true,
        (PartitionMode::CollectLeft, JoinType::Left | JoinType::Full) => right_partition_count == 1,
        (PartitionMode::Partitioned, JoinType::Inner | JoinType::Left | JoinType::Full) => true,
        _ => false,
    }
}

/// Immutable join metadata used by streamed execution.
struct JoinPlan {
    join_type: JoinType,
    left_on: Vec<usize>,
    right_on: Vec<usize>,
    filter: Option<JoinFilter>,
    output_schema: SchemaRef,
    right_schema: SchemaRef,
    projection: Option<Vec<usize>>,
}

/// Build-side state created once the collected left table is available.
///
/// `left_out` and `right_out` are the split projection maps used when probing
/// right-side batches and when finalizing unmatched build rows.
struct StreamingBuildSide {
    left: Arc<CuDFTable>,
    join: CuDFHashJoin,
    streaming_join: Option<CuDFStreamingJoin>,
    filter: Option<CuDFAstExpression>,
    left_out: Option<Vec<usize>>,
    right_out: Option<Vec<usize>>,
}

/// Lifecycle state for streamed joins.
///
/// The left side is materialized lazily on the first poll. Right-side batches
/// are then probed one at a time; `Left` and `Full` emit unmatched build rows
/// once at end of stream.
struct StreamingJoinState {
    plan: JoinPlan,
    left_fut: Option<SharedTableFuture>,
    build: Option<StreamingBuildSide>,
    right_stream: SendableRecordBatchStream,
    metrics: CuDFHashJoinMetrics,
    emitted_unmatched_build: bool,
}

impl StreamingJoinState {
    fn new(
        plan: JoinPlan,
        left_fut: SharedTableFuture,
        right_stream: SendableRecordBatchStream,
        metrics: CuDFHashJoinMetrics,
    ) -> Self {
        Self {
            plan,
            left_fut: Some(left_fut),
            build: None,
            right_stream,
            metrics,
            emitted_unmatched_build: false,
        }
    }

    /// Produce the next streamed output batch from one right-side input batch.
    ///
    /// For `Left` and `Full`, EOF triggers one final unmatched-build batch.
    async fn next_batch(
        mut self,
    ) -> Result<Option<(RecordBatch, StreamingJoinState)>, DataFusionError> {
        self.ensure_ready().await?;

        loop {
            let right_batch = {
                let _timer = self.metrics.probe_collect_time.timer();
                self.right_stream.next().await.transpose()?
            };
            let Some(right_batch) = right_batch else {
                if matches!(self.plan.join_type, JoinType::Left | JoinType::Full)
                    && !self.emitted_unmatched_build
                {
                    self.emitted_unmatched_build = true;
                    if let Some(batch) = self.unmatched_build_batch()? {
                        self.metrics.baseline.record_output(&batch);
                        return Ok(Some((batch, self)));
                    }
                }
                self.metrics.baseline.done();
                return Ok(None);
            };

            self.metrics
                .record_probe_input(std::slice::from_ref(&right_batch));

            if right_batch.num_rows() == 0 {
                continue;
            }

            let right = Arc::new(batches_to_table(std::slice::from_ref(&right_batch))?);
            let result = self.probe(right)?;
            let batch = result
                .into_view()
                .to_record_batch_with_schema(&self.plan.output_schema)
                .map_err(cudf_to_df)?;

            if batch.num_rows() == 0 {
                continue;
            }

            self.metrics.baseline.record_output(&batch);
            return Ok(Some((batch, self)));
        }
    }

    /// Initialize the collected build side exactly once for the streaming path.
    async fn ensure_ready(&mut self) -> Result<(), DataFusionError> {
        if self.build.is_some() {
            return Ok(());
        }

        let left = self
            .left_fut
            .take()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "left future should be present before first streamed join batch".to_string(),
                )
            })?
            .await?;
        let left_view = Arc::clone(&left).view();
        let (left_out, right_out) =
            split_join_projection(&self.plan.projection, left_view.num_columns());
        let filter = self
            .plan
            .filter
            .as_ref()
            .map(join_filter_to_cudf_ast)
            .transpose()?;
        let join = {
            let _timer = self.metrics.build_time.timer();
            execute_cudf(
                CuDFHashJoin::build(&left_view, &self.plan.left_on)
                    .null_equality(CuDFNullEquality::Unequal),
            )?
        };
        let streaming_join = if matches!(self.plan.join_type, JoinType::Left | JoinType::Full) {
            Some(join.clone().into_streaming_join())
        } else {
            None
        };

        self.build = Some(StreamingBuildSide {
            left,
            join,
            streaming_join,
            filter,
            left_out,
            right_out,
        });
        Ok(())
    }

    /// Probe one right-side batch against the reusable build-side hash table.
    fn probe(&mut self, right: Arc<CuDFTable>) -> Result<CuDFTable, DataFusionError> {
        let build = self.build.as_mut().ok_or_else(|| {
            DataFusionError::Internal("build side should be initialized before probing".to_string())
        })?;
        let left_view = Arc::clone(&build.left).view();
        let right_view = right.view();
        let _timer = self.metrics.join_time.timer();

        let result = match self.plan.join_type {
            JoinType::Inner => {
                let mut op = build
                    .join
                    .inner(&right_view, &self.plan.right_on)
                    .payloads(&left_view, &right_view);
                if let Some(cols) = build.left_out.as_deref() {
                    op = op.select_build(cols);
                }
                if let Some(cols) = build.right_out.as_deref() {
                    op = op.select_probe(cols);
                }
                if let Some(predicate) = build.filter.as_ref() {
                    op = op
                        .filter(predicate)
                        .condition_tables(&left_view, &right_view);
                }
                execute_cudf(op)
            }
            JoinType::Left => {
                let streaming = build.streaming_join.as_mut().ok_or_else(|| {
                    DataFusionError::Internal(
                        "streaming join state should exist for left join".to_string(),
                    )
                })?;
                let mut op = streaming
                    .inner(&right_view, &self.plan.right_on)
                    .payloads(&left_view, &right_view);
                if let Some(cols) = build.left_out.as_deref() {
                    op = op.select_build(cols);
                }
                if let Some(cols) = build.right_out.as_deref() {
                    op = op.select_probe(cols);
                }
                if let Some(predicate) = build.filter.as_ref() {
                    op = op
                        .filter(predicate)
                        .condition_tables(&left_view, &right_view);
                }
                execute_cudf(op)
            }
            JoinType::Full => {
                let streaming = build.streaming_join.as_mut().ok_or_else(|| {
                    DataFusionError::Internal(
                        "streaming join state should exist for full join".to_string(),
                    )
                })?;
                let mut op = streaming
                    .left(&right_view, &self.plan.right_on)
                    .payloads(&left_view, &right_view);
                if let Some(cols) = build.left_out.as_deref() {
                    op = op.select_build(cols);
                }
                if let Some(cols) = build.right_out.as_deref() {
                    op = op.select_probe(cols);
                }
                if let Some(predicate) = build.filter.as_ref() {
                    op = op
                        .filter(predicate)
                        .condition_tables(&left_view, &right_view);
                }
                execute_cudf(op)
            }
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "CuDFHashJoinExec: unsupported streaming join type {other:?}",
                )))
            }
        };

        result
    }

    /// Emit collected-left rows that never matched any streamed right batch.
    fn unmatched_build_batch(&self) -> Result<Option<RecordBatch>, DataFusionError> {
        let build = self.build.as_ref().ok_or_else(|| {
            DataFusionError::Internal(
                "build side should be initialized before finalizing".to_string(),
            )
        })?;
        let left_view = Arc::clone(&build.left).view();
        let empty_right = execute_cudf(CuDFTable::from_arrow_host(RecordBatch::new_empty(
            Arc::clone(&self.plan.right_schema),
        )))?;
        let right_view = empty_right.into_view();

        let result = {
            let _timer = self.metrics.join_time.timer();
            let streaming = build.streaming_join.as_ref().ok_or_else(|| {
                DataFusionError::Internal(
                    "streaming join state should exist before finalizing".to_string(),
                )
            })?;
            let mut op = streaming
                .unmatched_build_rows()
                .payloads(&left_view, &right_view);
            if let Some(cols) = build.left_out.as_deref() {
                op = op.select_build(cols);
            }
            if let Some(cols) = build.right_out.as_deref() {
                op = op.select_probe(cols);
            }
            execute_cudf(op)?
        };

        let batch = result
            .into_view()
            .to_record_batch_with_schema(&self.plan.output_schema)
            .map_err(cudf_to_df)?;
        if batch.num_rows() == 0 {
            Ok(None)
        } else {
            Ok(Some(batch))
        }
    }
}

#[derive(Clone)]
struct CuDFHashJoinMetrics {
    baseline: CuDFBaselineMetrics,
    build_time: Time,
    probe_collect_time: Time,
    join_time: Time,
    build_input_batches: Count,
    build_input_rows: Count,
    build_input_bytes: Count,
    probe_input_batches: Count,
    probe_input_rows: Count,
    probe_input_bytes: Count,
    join_input_bytes: Gauge,
}

impl CuDFHashJoinMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline: CuDFBaselineMetrics::new(metrics, partition),
            build_time: MetricBuilder::new(metrics).subset_time("build_time", partition),
            probe_collect_time: MetricBuilder::new(metrics)
                .subset_time("probe_collect_time", partition),
            join_time: MetricBuilder::new(metrics).subset_time("join_time", partition),
            build_input_batches: MetricBuilder::new(metrics)
                .counter("build_input_batches", partition),
            build_input_rows: MetricBuilder::new(metrics).counter("build_input_rows", partition),
            build_input_bytes: MetricBuilder::new(metrics).counter("build_input_bytes", partition),
            probe_input_batches: MetricBuilder::new(metrics)
                .counter("probe_input_batches", partition),
            probe_input_rows: MetricBuilder::new(metrics).counter("probe_input_rows", partition),
            probe_input_bytes: MetricBuilder::new(metrics).counter("probe_input_bytes", partition),
            join_input_bytes: MetricBuilder::new(metrics).gauge("join_input_bytes", partition),
        }
    }

    fn record_build_input(&self, batches: &[RecordBatch]) {
        let stats = batch_stats(batches);
        self.build_input_batches.add(stats.batches);
        self.build_input_rows.add(stats.rows);
        self.build_input_bytes.add(stats.bytes);
        self.join_input_bytes.add(stats.bytes);
    }

    fn record_probe_input(&self, batches: &[RecordBatch]) {
        let stats = batch_stats(batches);
        self.probe_input_batches.add(stats.batches);
        self.probe_input_rows.add(stats.rows);
        self.probe_input_bytes.add(stats.bytes);
        self.join_input_bytes.add(stats.bytes);
    }
}

struct BatchStats {
    batches: usize,
    rows: usize,
    bytes: usize,
}

fn batch_stats(batches: &[RecordBatch]) -> BatchStats {
    BatchStats {
        batches: batches.len(),
        rows: batches.iter().map(|b| b.num_rows()).sum(),
        bytes: batches.iter().map(|b| b.get_array_memory_size()).sum(),
    }
}

fn split_join_projection(
    projection: &Option<Vec<usize>>,
    left_width: usize,
) -> (Option<Vec<usize>>, Option<Vec<usize>>) {
    debug_assert!(
        projection
            .as_ref()
            .is_none_or(|p| p.windows(2).all(|w| w[0] < w[1])),
        "join projection indices must be strictly ascending"
    );

    match projection {
        None => (None, None),
        Some(proj) => {
            let left = proj.iter().filter(|&&i| i < left_width).copied().collect();
            let right = proj
                .iter()
                .filter(|&&i| i >= left_width)
                .map(|&i| i - left_width)
                .collect();
            (Some(left), Some(right))
        }
    }
}

/// Concat GPU-resident record batches into one table.
///
/// # Panics
///
/// Panics if any column in any batch is not a GPU-resident `CuDFColumnView`.
fn batches_to_table(batches: &[RecordBatch]) -> Result<CuDFTable, DataFusionError> {
    let views: Vec<CuDFTableView> = batches
        .iter()
        .map(CuDFTableView::from_record_batch)
        .collect::<Result<_, _>>()
        .map_err(cudf_to_df)?;
    execute_cudf(CuDFTable::concat(views))
}

/// Try to convert a `HashJoinExec` to GPU.
///
/// Returns `None` for unsupported configurations: non-column keys, non-equi
/// filters, unsupported join types, unsupported null equality, or partition
/// modes that the streamed GPU implementation cannot execute correctly.
pub fn try_as_cudf_hash_join(
    node: &HashJoinExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
    for (l, r) in node.on() {
        if l.as_any().downcast_ref::<Column>().is_none()
            || r.as_any().downcast_ref::<Column>().is_none()
        {
            return Ok(None);
        }
    }

    if node.null_equality() != NullEquality::NullEqualsNothing {
        return Ok(None);
    }

    if let Some(filter) = node.filter() {
        if !is_join_filter_supported_by_cudf_ast(filter)? {
            return Ok(None);
        }
    }

    if !supports_streaming_join(
        node.partition_mode(),
        *node.join_type(),
        node.right().output_partitioning().partition_count(),
    ) {
        return Ok(None);
    }

    Ok(Some(Arc::new(CuDFHashJoinExec::from_hash_join_exec(node)?)))
}

#[cfg(test)]
mod tests {
    use super::{cudf_schema_compatibility_map, try_as_cudf_hash_join, CuDFHashJoinExec};
    use crate::execution::execute_cudf;
    use crate::physical::{CuDFLoadExec, CuDFUnloadExec};
    use crate::planner::CuDFConfig;
    use arrow::array::{record_batch, Array, Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use datafusion::common::{JoinSide, JoinType, NullEquality};
    use datafusion::execution::TaskContext;
    use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
    use datafusion::prelude::SessionConfig;
    use datafusion_expr::Operator;
    use datafusion_physical_plan::expressions::{BinaryExpr, Column};
    use datafusion_physical_plan::joins::utils::{ColumnIndex, JoinFilter};
    use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion_physical_plan::test::TestMemoryExec;
    use datafusion_physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
    };
    use futures::stream;
    use futures_util::TryStreamExt;
    use libcudf_rs::CuDFTable;
    use std::any::Any;
    use std::error::Error;
    use std::fmt::Formatter;
    use std::sync::Arc;

    fn left_batch() -> RecordBatch {
        record_batch!(
            ("key", Int32, [1, 2, 3, 4]),
            ("val", Int32, [10, 20, 30, 40])
        )
        .unwrap()
    }

    fn right_batch() -> RecordBatch {
        record_batch!(("key", Int32, [2, 3, 5]), ("val", Int32, [200, 300, 500])).unwrap()
    }

    fn empty_right() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(Vec::<i32>::new())),
                Arc::new(Int32Array::from(Vec::<i32>::new())),
            ],
        )
        .unwrap()
    }

    fn key_on() -> Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> {
        vec![(
            Arc::new(Column::new("key", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("key", 0)) as Arc<dyn PhysicalExpr>,
        )]
    }

    async fn run_join(
        left: RecordBatch,
        right: RecordBatch,
        join_type: JoinType,
        partition_mode: PartitionMode,
    ) -> Result<Vec<RecordBatch>, Box<dyn Error>> {
        run_join_with_right_batches(left, vec![right], join_type, partition_mode).await
    }

    async fn run_join_with_right_batches(
        left: RecordBatch,
        right_batches: Vec<RecordBatch>,
        join_type: JoinType,
        partition_mode: PartitionMode,
    ) -> Result<Vec<RecordBatch>, Box<dyn Error>> {
        run_join_with_right_batches_and_filter(left, right_batches, join_type, partition_mode, None)
            .await
    }

    async fn run_join_with_right_batches_and_filter(
        left: RecordBatch,
        right_batches: Vec<RecordBatch>,
        join_type: JoinType,
        partition_mode: PartitionMode,
        filter: Option<JoinFilter>,
    ) -> Result<Vec<RecordBatch>, Box<dyn Error>> {
        run_join_with_partitions(
            vec![vec![left]],
            vec![right_batches],
            join_type,
            partition_mode,
            filter,
        )
        .await
    }

    async fn run_join_with_partitions(
        left_partitions: Vec<Vec<RecordBatch>>,
        right_partitions: Vec<Vec<RecordBatch>>,
        join_type: JoinType,
        partition_mode: PartitionMode,
        filter: Option<JoinFilter>,
    ) -> Result<Vec<RecordBatch>, Box<dyn Error>> {
        let left_schema = partition_schema(&left_partitions, left_batch().schema());
        let right_schema = partition_schema(&right_partitions, right_batch().schema());
        let partition_count = match partition_mode {
            PartitionMode::CollectLeft => right_partitions.len(),
            PartitionMode::Partitioned => {
                assert_eq!(
                    left_partitions.len(),
                    right_partitions.len(),
                    "partitioned joins require matching left/right partition counts"
                );
                left_partitions.len()
            }
            PartitionMode::Auto => unreachable!("Auto joins are not supported by CuDFHashJoinExec"),
        };

        let (left_in, right_in): (Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>) =
            match partition_mode {
                PartitionMode::CollectLeft => {
                    // CuDFLoadExec intentionally coalesces host input partitions into one GPU
                    // partition. CollectLeft tests use that production upload path.
                    let left = Arc::new(CuDFLoadExec::try_new(Arc::new(TestMemoryExec::try_new(
                        &left_partitions,
                        left_schema,
                        None,
                    )?))?);
                    let right = Arc::new(CuDFLoadExec::try_new(Arc::new(
                        TestMemoryExec::try_new(&right_partitions, right_schema, None)?,
                    ))?);
                    (left, right)
                }
                PartitionMode::Partitioned => {
                    // Partitioned join execution needs GPU-resident children that preserve
                    // partition counts; CuDFLoadExec is no longer that shape.
                    let left = Arc::new(TestGpuExec::try_new(left_partitions, left_schema)?);
                    let right = Arc::new(TestGpuExec::try_new(right_partitions, right_schema)?);
                    (left, right)
                }
                PartitionMode::Auto => {
                    unreachable!("Auto joins are not supported by CuDFHashJoinExec")
                }
            };
        let exec = CuDFHashJoinExec::try_new(
            left_in,
            right_in,
            key_on(),
            filter,
            join_type,
            None,
            partition_mode,
        )?;
        let unload = CuDFUnloadExec::new(Arc::new(exec));

        let mut out = Vec::new();
        for partition in 0..partition_count {
            let stream = unload.execute(partition, cudf_task_context())?;
            out.extend(stream.try_collect::<Vec<_>>().await?);
        }
        Ok(out)
    }

    fn cudf_task_context() -> Arc<TaskContext> {
        Arc::new(TaskContext::default().with_session_config(
            SessionConfig::default().with_option_extension(CuDFConfig::default()),
        ))
    }

    #[derive(Debug)]
    struct TestGpuExec {
        partitions: Vec<Vec<RecordBatch>>,
        properties: Arc<PlanProperties>,
    }

    impl TestGpuExec {
        fn try_new(
            partitions: Vec<Vec<RecordBatch>>,
            schema: SchemaRef,
        ) -> Result<Self, Box<dyn Error>> {
            let input = TestMemoryExec::try_new(&partitions, schema, None)?;
            let properties = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(cudf_schema_compatibility_map(input.schema())),
                Partitioning::UnknownPartitioning(partitions.len()),
                input.properties().emission_type,
                input.properties().boundedness,
            ));
            Ok(Self {
                partitions,
                properties,
            })
        }
    }

    impl DisplayAs for TestGpuExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "TestGpuExec")
        }
    }

    impl ExecutionPlan for TestGpuExec {
        fn name(&self) -> &str {
            "TestGpuExec"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            if !children.is_empty() {
                return datafusion::common::plan_err!(
                    "TestGpuExec expects no children, {} were provided",
                    children.len()
                );
            }
            Ok(self)
        }

        fn execute(
            &self,
            partition: usize,
            _context: Arc<TaskContext>,
        ) -> datafusion::common::Result<datafusion::execution::SendableRecordBatchStream> {
            let Some(batches) = self.partitions.get(partition).cloned() else {
                return datafusion::common::internal_err!(
                    "TestGpuExec invalid partition {partition}"
                );
            };
            let schema = self.schema();
            let stream = stream::iter(
                batches
                    .into_iter()
                    .map(move |batch| host_batch_to_gpu(batch, Arc::clone(&schema))),
            );
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema(),
                stream,
            )))
        }
    }

    fn host_batch_to_gpu(
        batch: RecordBatch,
        schema: SchemaRef,
    ) -> datafusion::common::Result<RecordBatch> {
        let table = execute_cudf(CuDFTable::from_arrow_host(batch))?;
        let num_rows = table.num_rows();
        let columns = table
            .into_columns()
            .into_iter()
            .map(|column| Arc::new(column.into_view()) as Arc<dyn Array>)
            .collect();
        Ok(libcudf_rs::record_batch_with_schema(
            columns, &schema, num_rows,
        )?)
    }

    fn partition_schema(partitions: &[Vec<RecordBatch>], default: SchemaRef) -> SchemaRef {
        partitions
            .iter()
            .flatten()
            .next()
            .map(RecordBatch::schema)
            .unwrap_or(default)
    }

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    fn total_nulls(batches: &[RecordBatch], column: usize) -> usize {
        batches.iter().map(|b| b.column(column).null_count()).sum()
    }

    fn int32_options(batches: &[RecordBatch], column: usize) -> Vec<Option<i32>> {
        batches
            .iter()
            .flat_map(|batch| {
                let values = batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                (0..values.len()).map(move |i| {
                    if values.is_null(i) {
                        None
                    } else {
                        Some(values.value(i))
                    }
                })
            })
            .collect()
    }

    fn right_batch_from_rows(rows: &[(i32, i32)]) -> Result<RecordBatch, Box<dyn Error>> {
        let keys: Vec<_> = rows.iter().map(|(key, _)| *key).collect();
        let vals: Vec<_> = rows.iter().map(|(_, val)| *val).collect();
        Ok(RecordBatch::try_new(
            right_batch().schema(),
            vec![
                Arc::new(Int32Array::from(keys)),
                Arc::new(Int32Array::from(vals)),
            ],
        )?)
    }

    fn partitioned_left_batches() -> Result<Vec<Vec<RecordBatch>>, Box<dyn Error>> {
        Ok(vec![
            vec![right_batch_from_rows(&[(1, 10), (2, 20)])?],
            vec![right_batch_from_rows(&[(3, 30), (4, 40)])?],
        ])
    }

    fn partitioned_right_batches() -> Result<Vec<Vec<RecordBatch>>, Box<dyn Error>> {
        Ok(vec![
            vec![right_batch_from_rows(&[(2, 200), (5, 500)])?],
            vec![right_batch_from_rows(&[(3, 300), (6, 600)])?],
        ])
    }

    fn value_less_join_filter() -> JoinFilter {
        let filter_schema = Arc::new(Schema::new(vec![
            Field::new("left_val", DataType::Int32, false),
            Field::new("right_val", DataType::Int32, false),
        ]));
        JoinFilter::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("left_val", 0)),
                Operator::Lt,
                Arc::new(Column::new("right_val", 1)),
            )) as Arc<dyn PhysicalExpr>,
            vec![
                ColumnIndex {
                    index: 1,
                    side: JoinSide::Left,
                },
                ColumnIndex {
                    index: 1,
                    side: JoinSide::Right,
                },
            ],
            filter_schema,
        )
    }

    async fn assert_streamed_inner_join(
        partition_mode: PartitionMode,
    ) -> Result<(), Box<dyn Error>> {
        let out = run_join_with_right_batches(
            left_batch(),
            vec![
                right_batch_from_rows(&[(2, 200)])?,
                right_batch_from_rows(&[(3, 300)])?,
            ],
            JoinType::Inner,
            partition_mode,
        )
        .await?;

        assert!(!out.is_empty());
        assert_eq!(total_rows(&out), 2);
        Ok(())
    }

    async fn assert_streamed_left_join(
        partition_mode: PartitionMode,
    ) -> Result<(), Box<dyn Error>> {
        let out = run_join_with_right_batches(
            left_batch(),
            vec![
                right_batch_from_rows(&[(2, 200)])?,
                right_batch_from_rows(&[(3, 300)])?,
            ],
            JoinType::Left,
            partition_mode,
        )
        .await?;

        assert!(!out.is_empty());
        assert_eq!(total_rows(&out), 4);
        assert_eq!(total_nulls(&out, 2), 2);
        Ok(())
    }

    async fn assert_streamed_full_join(
        partition_mode: PartitionMode,
    ) -> Result<(), Box<dyn Error>> {
        let out = run_join_with_right_batches(
            left_batch(),
            vec![
                right_batch_from_rows(&[(2, 200), (5, 500)])?,
                right_batch_from_rows(&[(3, 300), (6, 600)])?,
            ],
            JoinType::Full,
            partition_mode,
        )
        .await?;

        assert!(!out.is_empty());
        assert_eq!(total_rows(&out), 6);
        assert_eq!(total_nulls(&out, 0), 2);
        assert_eq!(total_nulls(&out, 2), 2);
        Ok(())
    }

    async fn assert_streamed_left_join_with_duplicate_matches(
        partition_mode: PartitionMode,
    ) -> Result<(), Box<dyn Error>> {
        let out = run_join_with_right_batches(
            left_batch(),
            vec![
                right_batch_from_rows(&[(2, 200), (2, 201)])?,
                right_batch_from_rows(&[(3, 300), (3, 301), (5, 500)])?,
            ],
            JoinType::Left,
            partition_mode,
        )
        .await?;

        assert!(!out.is_empty());
        assert_eq!(total_rows(&out), 6);
        assert_eq!(total_nulls(&out, 0), 0);
        assert_eq!(total_nulls(&out, 2), 2);
        Ok(())
    }

    async fn assert_streamed_full_join_with_duplicate_matches(
        partition_mode: PartitionMode,
    ) -> Result<(), Box<dyn Error>> {
        let out = run_join_with_right_batches(
            left_batch(),
            vec![
                right_batch_from_rows(&[(2, 200), (2, 201)])?,
                right_batch_from_rows(&[(3, 300), (3, 301), (5, 500)])?,
            ],
            JoinType::Full,
            partition_mode,
        )
        .await?;

        assert!(!out.is_empty());
        assert_eq!(total_rows(&out), 7);
        assert_eq!(total_nulls(&out, 0), 1);
        assert_eq!(total_nulls(&out, 2), 2);
        Ok(())
    }

    fn conversion_join(
        join_type: JoinType,
        partition_mode: PartitionMode,
        right_partitions: &[Vec<RecordBatch>],
    ) -> Result<HashJoinExec, Box<dyn Error>> {
        conversion_join_with_null_equality(
            join_type,
            partition_mode,
            right_partitions,
            NullEquality::NullEqualsNothing,
        )
    }

    fn conversion_join_with_null_equality(
        join_type: JoinType,
        partition_mode: PartitionMode,
        right_partitions: &[Vec<RecordBatch>],
        null_equality: NullEquality,
    ) -> Result<HashJoinExec, Box<dyn Error>> {
        let schema = left_batch().schema();
        let left = Arc::new(TestMemoryExec::try_new(
            &[vec![left_batch()]],
            schema.clone(),
            None,
        )?);
        let right = Arc::new(TestMemoryExec::try_new(
            right_partitions,
            schema.clone(),
            None,
        )?);
        Ok(HashJoinExec::try_new(
            left,
            right,
            key_on(),
            None,
            &join_type,
            None,
            partition_mode,
            null_equality,
            false,
        )?)
    }

    fn one_right_partition() -> Result<Vec<Vec<RecordBatch>>, Box<dyn Error>> {
        Ok(vec![vec![right_batch_from_rows(&[(2, 200)])?]])
    }

    fn two_right_partitions() -> Result<Vec<Vec<RecordBatch>>, Box<dyn Error>> {
        Ok(vec![
            vec![right_batch_from_rows(&[(2, 200)])?],
            vec![right_batch_from_rows(&[(3, 300)])?],
        ])
    }

    #[tokio::test]
    async fn test_inner_join() -> Result<(), Box<dyn Error>> {
        let out = run_join(
            left_batch(),
            right_batch(),
            JoinType::Inner,
            PartitionMode::CollectLeft,
        )
        .await?;
        assert_eq!(total_rows(&out), 2); // keys 2 and 3 match
        assert_eq!(out[0].num_columns(), 4);
        Ok(())
    }

    #[tokio::test]
    async fn test_filtered_inner_join() -> Result<(), Box<dyn Error>> {
        let out = run_join_with_right_batches_and_filter(
            left_batch(),
            vec![right_batch_from_rows(&[(2, 25), (3, 25), (5, 500)])?],
            JoinType::Inner,
            PartitionMode::CollectLeft,
            Some(value_less_join_filter()),
        )
        .await?;

        assert_eq!(total_rows(&out), 1);
        assert_eq!(int32_options(&out, 1), vec![Some(20)]);
        assert_eq!(int32_options(&out, 3), vec![Some(25)]);
        Ok(())
    }

    #[tokio::test]
    async fn test_inner_join_empty_right() -> Result<(), Box<dyn Error>> {
        let out = run_join(
            left_batch(),
            empty_right(),
            JoinType::Inner,
            PartitionMode::CollectLeft,
        )
        .await?;
        assert_eq!(total_rows(&out), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_collect_left_inner_join_streams_right_batches() -> Result<(), Box<dyn Error>> {
        assert_streamed_inner_join(PartitionMode::CollectLeft).await
    }

    #[tokio::test]
    async fn test_collect_left_left_join_streams_and_finalizes_unmatched(
    ) -> Result<(), Box<dyn Error>> {
        assert_streamed_left_join(PartitionMode::CollectLeft).await
    }

    #[tokio::test]
    async fn test_collect_left_full_join_streams_and_finalizes_unmatched(
    ) -> Result<(), Box<dyn Error>> {
        assert_streamed_full_join(PartitionMode::CollectLeft).await
    }

    #[tokio::test]
    async fn test_left_join_duplicate_matches_update_mask_once() -> Result<(), Box<dyn Error>> {
        for partition_mode in [PartitionMode::CollectLeft, PartitionMode::Partitioned] {
            assert_streamed_left_join_with_duplicate_matches(partition_mode).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_full_join_duplicate_matches_update_mask_once() -> Result<(), Box<dyn Error>> {
        for partition_mode in [PartitionMode::CollectLeft, PartitionMode::Partitioned] {
            assert_streamed_full_join_with_duplicate_matches(partition_mode).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_partitioned_inner_join_executes_all_partitions() -> Result<(), Box<dyn Error>> {
        let out = run_join_with_partitions(
            partitioned_left_batches()?,
            partitioned_right_batches()?,
            JoinType::Inner,
            PartitionMode::Partitioned,
            None,
        )
        .await?;

        assert_eq!(out.len(), 2);
        assert_eq!(total_rows(&out), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_partitioned_left_join_executes_all_partitions_and_finalizes_unmatched(
    ) -> Result<(), Box<dyn Error>> {
        let out = run_join_with_partitions(
            partitioned_left_batches()?,
            partitioned_right_batches()?,
            JoinType::Left,
            PartitionMode::Partitioned,
            None,
        )
        .await?;

        assert_eq!(out.len(), 4);
        assert_eq!(total_rows(&out), 4);
        assert_eq!(total_nulls(&out, 2), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_partitioned_full_join_executes_all_partitions_and_finalizes_unmatched(
    ) -> Result<(), Box<dyn Error>> {
        let out = run_join_with_partitions(
            partitioned_left_batches()?,
            partitioned_right_batches()?,
            JoinType::Full,
            PartitionMode::Partitioned,
            None,
        )
        .await?;

        assert_eq!(out.len(), 4);
        assert_eq!(total_rows(&out), 6);
        assert_eq!(total_nulls(&out, 0), 2);
        assert_eq!(total_nulls(&out, 2), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_left_join_no_right_batches() -> Result<(), Box<dyn Error>> {
        let out = run_join_with_right_batches(
            left_batch(),
            Vec::new(),
            JoinType::Left,
            PartitionMode::CollectLeft,
        )
        .await?;

        assert_eq!(total_rows(&out), 4);
        assert_eq!(out[0].num_columns(), 4);
        assert_eq!(total_nulls(&out, 2), 4);
        assert_eq!(total_nulls(&out, 3), 4);
        Ok(())
    }

    #[tokio::test]
    async fn test_filtered_left_join_preserves_failed_build_rows() -> Result<(), Box<dyn Error>> {
        let out = run_join_with_right_batches_and_filter(
            left_batch(),
            vec![right_batch_from_rows(&[(2, 25), (3, 25), (5, 500)])?],
            JoinType::Left,
            PartitionMode::CollectLeft,
            Some(value_less_join_filter()),
        )
        .await?;

        let mut key_pairs: Vec<_> = int32_options(&out, 0)
            .into_iter()
            .zip(int32_options(&out, 2))
            .collect();
        key_pairs.sort();
        assert_eq!(
            key_pairs,
            vec![
                (Some(1), None),
                (Some(2), Some(2)),
                (Some(3), None),
                (Some(4), None),
            ]
        );
        assert_eq!(total_nulls(&out, 2), 3);
        Ok(())
    }

    #[tokio::test]
    async fn test_left_join() -> Result<(), Box<dyn Error>> {
        let out = run_join(
            left_batch(),
            right_batch(),
            JoinType::Left,
            PartitionMode::CollectLeft,
        )
        .await?;
        assert_eq!(total_rows(&out), 4); // all 4 left rows preserved
        assert_eq!(out[0].num_columns(), 4);
        Ok(())
    }

    #[tokio::test]
    async fn test_filtered_full_join_handles_failed_and_duplicate_matches(
    ) -> Result<(), Box<dyn Error>> {
        let out = run_join_with_right_batches_and_filter(
            right_batch_from_rows(&[(2, 20), (2, 30), (4, 40)])?,
            vec![right_batch_from_rows(&[(2, 25), (5, 50)])?],
            JoinType::Full,
            PartitionMode::CollectLeft,
            Some(value_less_join_filter()),
        )
        .await?;

        let mut value_pairs: Vec<_> = int32_options(&out, 1)
            .into_iter()
            .zip(int32_options(&out, 3))
            .collect();
        value_pairs.sort();
        assert_eq!(
            value_pairs,
            vec![
                (None, Some(50)),
                (Some(20), Some(25)),
                (Some(30), None),
                (Some(40), None),
            ]
        );
        assert_eq!(total_nulls(&out, 0), 1);
        assert_eq!(total_nulls(&out, 2), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_full_join() -> Result<(), Box<dyn Error>> {
        let out = run_join(
            left_batch(),
            right_batch(),
            JoinType::Full,
            PartitionMode::CollectLeft,
        )
        .await?;
        // 2 matches + 2 left-only + 1 right-only = 5
        assert_eq!(total_rows(&out), 5);
        assert_eq!(out[0].num_columns(), 4);
        Ok(())
    }

    #[test]
    fn test_conversion_with_narrowed_child_schema() -> Result<(), Box<dyn Error>> {
        // Three-table schema: ll join lr on key=key, then (ll join lr) join r on outer_key=outer_key.
        let ll_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]));
        let lr_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("outer_key", DataType::Int32, false),
        ]));
        let r_schema = Arc::new(Schema::new(vec![
            Field::new("outer_key", DataType::Int32, false),
            Field::new("result", DataType::Int32, false),
        ]));

        let ll = Arc::new(TestMemoryExec::try_new(&[], ll_schema, None)?);
        let lr = Arc::new(TestMemoryExec::try_new(&[], lr_schema, None)?);
        let r = Arc::new(TestMemoryExec::try_new(&[], r_schema, None)?);

        let inner_on = vec![(
            Arc::new(Column::new("key", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("key", 0)) as Arc<dyn PhysicalExpr>,
        )];

        // Inner join without projection; full output: [ll.key(0), ll.val(1), lr.key(2), lr.outer_key(3)].
        let inner_full = HashJoinExec::try_new(
            ll.clone(),
            lr.clone(),
            inner_on.clone(),
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )?;

        // Outer join references lr.outer_key at index 3 of the full inner output.
        let outer_on = vec![(
            Arc::new(Column::new("outer_key", 3)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("outer_key", 0)) as Arc<dyn PhysicalExpr>,
        )];
        let outer_join = Arc::new(HashJoinExec::try_new(
            Arc::new(inner_full),
            r.clone(),
            outer_on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )?);

        // Optimizer adds projection=[0,1] to the inner join, narrowing its output to
        // [ll.key, ll.val] and dropping lr.outer_key.
        let inner_projected = HashJoinExec::try_new(
            ll,
            lr,
            inner_on,
            None,
            &JoinType::Inner,
            Some(vec![0, 1]),
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )?;
        let cudf_inner = Arc::new(CuDFHashJoinExec::from_hash_join_exec(&inner_projected)?);

        // DataFusion validates on-key columns when replacing children, so this
        // fails before the GPU conversion gets another chance to run.
        let outer_narrowed = outer_join.with_new_children(vec![cudf_inner, r]);
        assert!(outer_narrowed.is_err());
        Ok(())
    }

    #[test]
    fn test_non_boolean_join_filter_bails_to_cpu() -> Result<(), Box<dyn Error>> {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
        let left = Arc::new(TestMemoryExec::try_new(&[], schema.clone(), None)?);
        let right = Arc::new(TestMemoryExec::try_new(&[], schema.clone(), None)?);
        let filter = JoinFilter::new(
            Arc::new(Column::new("key", 0)) as Arc<dyn PhysicalExpr>,
            vec![ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            }],
            Arc::clone(&schema),
        );
        let join = HashJoinExec::try_new(
            left,
            right,
            key_on(),
            Some(filter),
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )?;
        assert!(try_as_cudf_hash_join(&join)?.is_none());
        Ok(())
    }

    #[test]
    fn test_supported_join_filter_converts_to_gpu() -> Result<(), Box<dyn Error>> {
        let schema = left_batch().schema();
        let left = Arc::new(TestMemoryExec::try_new(
            &[vec![left_batch()]],
            schema.clone(),
            None,
        )?);
        let right = Arc::new(TestMemoryExec::try_new(
            &one_right_partition()?,
            schema,
            None,
        )?);
        let join = HashJoinExec::try_new(
            left,
            right,
            key_on(),
            Some(value_less_join_filter()),
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )?;
        assert!(try_as_cudf_hash_join(&join)?.is_some());
        Ok(())
    }

    #[test]
    fn test_supported_join_shapes_convert_to_gpu() -> Result<(), Box<dyn Error>> {
        for (join_type, partition_mode, right_partitions) in [
            (
                JoinType::Inner,
                PartitionMode::CollectLeft,
                two_right_partitions()?,
            ),
            (
                JoinType::Left,
                PartitionMode::CollectLeft,
                one_right_partition()?,
            ),
            (
                JoinType::Full,
                PartitionMode::CollectLeft,
                one_right_partition()?,
            ),
            (
                JoinType::Inner,
                PartitionMode::Partitioned,
                two_right_partitions()?,
            ),
            (
                JoinType::Left,
                PartitionMode::Partitioned,
                two_right_partitions()?,
            ),
            (
                JoinType::Full,
                PartitionMode::Partitioned,
                two_right_partitions()?,
            ),
        ] {
            let join = conversion_join(join_type, partition_mode, &right_partitions)?;
            assert!(try_as_cudf_hash_join(&join)?.is_some());
        }
        Ok(())
    }

    #[test]
    fn test_unsupported_join_shapes_bail_to_cpu() -> Result<(), Box<dyn Error>> {
        for (join_type, partition_mode, right_partitions) in [
            (JoinType::Inner, PartitionMode::Auto, one_right_partition()?),
            (
                JoinType::LeftSemi,
                PartitionMode::CollectLeft,
                one_right_partition()?,
            ),
            (
                JoinType::Left,
                PartitionMode::CollectLeft,
                two_right_partitions()?,
            ),
            (
                JoinType::Full,
                PartitionMode::CollectLeft,
                two_right_partitions()?,
            ),
        ] {
            let join = conversion_join(join_type, partition_mode, &right_partitions)?;
            assert!(try_as_cudf_hash_join(&join)?.is_none());
        }

        let null_equals_join = conversion_join_with_null_equality(
            JoinType::Inner,
            PartitionMode::CollectLeft,
            &one_right_partition()?,
            NullEquality::NullEqualsNull,
        )?;
        assert!(try_as_cudf_hash_join(&null_equals_join)?.is_none());
        Ok(())
    }

    #[test]
    fn test_direct_constructor_rejects_unsupported_streaming_shape() -> Result<(), Box<dyn Error>> {
        let schema = left_batch().schema();
        let left = Arc::new(TestMemoryExec::try_new(
            &[vec![left_batch()]],
            schema.clone(),
            None,
        )?);
        let right = Arc::new(TestMemoryExec::try_new(
            &two_right_partitions()?,
            schema,
            None,
        )?);

        let result = CuDFHashJoinExec::try_new(
            left,
            right,
            key_on(),
            None,
            JoinType::Left,
            None,
            PartitionMode::CollectLeft,
        );
        assert!(result.is_err());
        Ok(())
    }
}

#[cfg(test)]
mod integration {
    use crate::test_utils::TestFramework;
    use datafusion::common::assert_contains;
    use std::error::Error;

    async fn check(sql: &str) -> Result<(), Box<dyn Error>> {
        let tf = TestFramework::new().await;
        let cudf = tf.execute(&format!("SET cudf.enable=true; {sql}")).await?;
        let cpu = tf.execute(sql).await?;
        assert_contains!(&cudf.plan, "CuDFHashJoinExec");
        assert_eq!(cpu.pretty_print, cudf.pretty_print);
        Ok(())
    }

    async fn check_correct(sql: &str) -> Result<(), Box<dyn Error>> {
        let tf = TestFramework::new().await;
        let cudf = tf.execute(&format!("SET cudf.enable=true; {sql}")).await?;
        let cpu = tf.execute(sql).await?;
        assert_eq!(cpu.pretty_print, cudf.pretty_print);
        Ok(())
    }

    async fn check_filtered_join_sql(sql: &str) -> Result<(), Box<dyn Error>> {
        let setup = r#"
            CREATE TABLE filter_left (k INT, v INT) AS VALUES
                (1, 10), (2, 20), (2, 30), (4, 40);
            CREATE TABLE filter_right (k INT, v INT) AS VALUES
                (2, 25), (3, 35), (5, 50), (6, 60), (7, 70)
        "#;

        let cudf_tf = TestFramework::new().await;
        let cpu_tf = TestFramework::new().await;
        let cudf = cudf_tf
            .execute(&format!("SET cudf.enable=true; {setup}; {sql}"))
            .await?;
        let cpu = cpu_tf.execute(&format!("{setup}; {sql}")).await?;
        assert_contains!(&cudf.plan, "CuDFHashJoinExec");
        assert_contains!(&cudf.plan, "filter=");
        assert_eq!(cpu.pretty_print, cudf.pretty_print);
        Ok(())
    }

    #[tokio::test]
    async fn test_inner_join() -> Result<(), Box<dyn Error>> {
        check(
            r#"SELECT a."MinTemp", b."MaxTemp" FROM weather a
               JOIN weather b ON a."MinTemp" = b."MinTemp"
               ORDER BY a."MinTemp", b."MaxTemp" LIMIT 10"#,
        )
        .await
    }

    #[tokio::test]
    async fn test_inner_join_multi_key() -> Result<(), Box<dyn Error>> {
        check(
            r#"SELECT a."MinTemp", a."MaxTemp" FROM weather a
               JOIN weather b ON a."MinTemp" = b."MinTemp" AND a."MaxTemp" = b."MaxTemp"
               ORDER BY a."MinTemp", a."MaxTemp" LIMIT 10"#,
        )
        .await
    }

    #[tokio::test]
    async fn test_filtered_inner_join_sql() -> Result<(), Box<dyn Error>> {
        check_filtered_join_sql(
            r#"
            SELECT l.k AS lk, l.v AS lv, r.k AS rk, r.v AS rv
            FROM filter_left l
            JOIN filter_right r ON l.k = r.k AND l.v < r.v
            ORDER BY lk, lv, rk, rv
            "#,
        )
        .await
    }

    #[tokio::test]
    async fn test_filtered_left_join_sql() -> Result<(), Box<dyn Error>> {
        check_filtered_join_sql(
            r#"
            SELECT l.k AS lk, l.v AS lv, r.k AS rk, r.v AS rv
            FROM filter_left l
            LEFT JOIN filter_right r ON l.k = r.k AND l.v < r.v
            ORDER BY lk, lv, rk, rv
            "#,
        )
        .await
    }

    #[tokio::test]
    async fn test_filtered_full_join_sql() -> Result<(), Box<dyn Error>> {
        check_filtered_join_sql(
            r#"
            SELECT l.k AS lk, l.v AS lv, r.k AS rk, r.v AS rv
            FROM filter_left l
            FULL JOIN filter_right r ON l.k = r.k AND l.v < r.v
            ORDER BY COALESCE(l.k, r.k), l.v, r.v
            "#,
        )
        .await
    }

    #[tokio::test]
    async fn test_full_join() -> Result<(), Box<dyn Error>> {
        check_correct(
            r#"SELECT a."MinTemp", b."MaxTemp" FROM weather a
               FULL JOIN weather b ON a."MinTemp" = b."MinTemp"
               ORDER BY a."MinTemp", b."MaxTemp" LIMIT 10"#,
        )
        .await
    }
}
