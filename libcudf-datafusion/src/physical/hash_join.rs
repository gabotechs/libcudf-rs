use crate::errors::cudf_to_df;
use crate::metrics::CuDFBaselineMetrics;
use crate::physical::cudf_load::cudf_schema_compatibility_map;
use arrow::array::RecordBatch;
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::common::{JoinType, NullEquality, Statistics};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion_physical_plan::expressions::Column;
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
    full_join, inner_join, left_join, CuDFHashJoin, CuDFNullEquality, CuDFTable, CuDFTableView,
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

        let left_len = left_schema.fields().len();
        let right_len = right_schema.fields().len();
        for (l, r) in left_on.iter().zip(&right_on) {
            if *l >= left_len || *r >= right_len {
                return datafusion::common::plan_err!(
                    "CuDFHashJoinExec: on-key index out of bounds (left={l}/{left_len}, right={r}/{right_len})"
                );
            }
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
        )
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
        // Partitioned/Auto: each partition builds its own left table independently.
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
                    batches_to_table(&batches).map(Arc::new).map_err(cudf_to_df)
                })
            }
        };

        let join_type = self.join_type;
        let left_on = self.left_on.clone();
        let right_on = self.right_on.clone();
        let projection = self.projection.clone();
        let output_schema = self.schema();
        let right_schema = self.right.schema();

        if matches!(self.partition_mode, PartitionMode::CollectLeft)
            && matches!(join_type, JoinType::Inner)
        {
            let stream = futures::stream::try_unfold(
                InnerJoinStreamState {
                    left_fut: Some(left_fut),
                    left: None,
                    join: None,
                    right_stream,
                    metrics,
                    left_on,
                    right_on,
                    output_schema: output_schema.clone(),
                    projection,
                    left_out: None,
                    right_out: None,
                },
                next_inner_join_batch,
            );
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                output_schema,
                stream,
            )));
        }

        let stream = futures::stream::once(async move {
            let left = left_fut.await?;
            let right_batches: Vec<RecordBatch> = {
                let _timer = metrics.probe_collect_time.timer();
                let batches: Vec<RecordBatch> = right_stream.try_collect().await?;
                metrics.record_probe_input(&batches);
                batches
            };

            let right_empty =
                right_batches.is_empty() || right_batches.iter().all(|b| b.num_rows() == 0);

            // Inner join with no right rows: no matches possible, emit nothing.
            if matches!(join_type, JoinType::Inner) && right_empty {
                return Ok(None);
            }

            // For Left/Full joins with no right batches, synthesize an empty right table
            // from the child schema so the join kernel returns all left rows with null
            // right columns.
            let right = if right_batches.is_empty() {
                let empty = RecordBatch::new_empty(right_schema);
                Arc::new(CuDFTable::from_arrow_host(empty).map_err(cudf_to_df)?)
            } else {
                Arc::new(batches_to_table(&right_batches).map_err(cudf_to_df)?)
            };

            let result = {
                let _timer = metrics.join_time.timer();
                perform_join(
                    left,
                    right,
                    join_type,
                    &left_on,
                    &right_on,
                    &output_schema,
                    &projection,
                )
            };
            if let Ok(Some(batch)) = &result {
                metrics.baseline.record_output(batch);
            }
            result
        })
        .filter_map(|result| async move {
            match result {
                Ok(Some(batch)) => Some(Ok(batch)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
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
                batches_to_table(&batches)
                    .map(Arc::new)
                    .map_err(|e| Arc::new(cudf_to_df(e)))
            })
            .await
            .map(Arc::clone)
            .map_err(|e: Arc<DataFusionError>| DataFusionError::External(Box::new(e)))
    })
}

struct InnerJoinStreamState {
    left_fut: Option<SharedTableFuture>,
    left: Option<Arc<CuDFTable>>,
    join: Option<CuDFHashJoin>,
    right_stream: SendableRecordBatchStream,
    metrics: CuDFHashJoinMetrics,
    left_on: Vec<usize>,
    right_on: Vec<usize>,
    output_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    left_out: Option<Vec<usize>>,
    right_out: Option<Vec<usize>>,
}

async fn ensure_inner_join_ready(state: &mut InnerJoinStreamState) -> Result<(), DataFusionError> {
    if state.join.is_some() {
        return Ok(());
    }

    let left = state
        .left_fut
        .take()
        .expect("left future should be present before first streamed join batch")
        .await?;
    let left_view = Arc::clone(&left).view();
    let (left_out, right_out) = split_join_projection(&state.projection, left_view.num_columns());
    let join = {
        let _timer = state.metrics.build_time.timer();
        CuDFHashJoin::try_new(&left_view, &state.left_on, CuDFNullEquality::Unequal)
            .map_err(cudf_to_df)?
    };

    state.left = Some(left);
    state.join = Some(join);
    state.left_out = left_out;
    state.right_out = right_out;
    Ok(())
}

async fn next_inner_join_batch(
    mut state: InnerJoinStreamState,
) -> Result<Option<(RecordBatch, InnerJoinStreamState)>, DataFusionError> {
    ensure_inner_join_ready(&mut state).await?;

    loop {
        let right_batch = {
            let _timer = state.metrics.probe_collect_time.timer();
            state.right_stream.next().await.transpose()?
        };
        let Some(right_batch) = right_batch else {
            state.metrics.baseline.done();
            return Ok(None);
        };

        state
            .metrics
            .record_probe_input(std::slice::from_ref(&right_batch));

        if right_batch.num_rows() == 0 {
            continue;
        }

        let right =
            Arc::new(batches_to_table(std::slice::from_ref(&right_batch)).map_err(cudf_to_df)?);
        let result = {
            let left = state
                .left
                .as_ref()
                .expect("left table should be initialized before probing");
            let join = state
                .join
                .as_ref()
                .expect("hash join should be initialized before probing");
            let left_view = Arc::clone(left).view();
            let right_view = Arc::clone(&right).view();
            let _timer = state.metrics.join_time.timer();
            join.inner_join(
                &right_view,
                &state.right_on,
                &left_view,
                &right_view,
                state.left_out.as_deref(),
                state.right_out.as_deref(),
            )
            .map_err(cudf_to_df)?
        };
        let batch = result
            .into_view()
            .to_record_batch_with_schema(&state.output_schema)
            .map_err(cudf_to_df)?;

        if batch.num_rows() == 0 {
            continue;
        }

        state.metrics.baseline.record_output(&batch);
        return Ok(Some((batch, state)));
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

/// Run the cuDF join kernel and apply the output projection. Returns `None`
/// for inner joins with no matching rows.
fn perform_join(
    left: Arc<CuDFTable>,
    right: Arc<CuDFTable>,
    join_type: JoinType,
    left_on: &[usize],
    right_on: &[usize],
    output_schema: &SchemaRef,
    projection: &Option<Vec<usize>>,
) -> Result<Option<RecordBatch>, DataFusionError> {
    let left_view = left.view();
    let right_view = right.view();

    let (left_out, right_out) = split_join_projection(projection, left_view.num_columns());

    let result = match join_type {
        JoinType::Inner => inner_join(
            &left_view,
            &right_view,
            left_on,
            right_on,
            left_out.as_deref(),
            right_out.as_deref(),
        ),
        JoinType::Left => left_join(
            &left_view,
            &right_view,
            left_on,
            right_on,
            left_out.as_deref(),
            right_out.as_deref(),
        ),
        JoinType::Full => full_join(
            &left_view,
            &right_view,
            left_on,
            right_on,
            left_out.as_deref(),
            right_out.as_deref(),
        ),
        other => {
            return Err(DataFusionError::NotImplemented(format!(
                "CuDFHashJoinExec: unsupported join type {other:?}"
            )))
        }
    }
    .map_err(cudf_to_df)?;

    let batch = result
        .into_view()
        .to_record_batch_with_schema(output_schema)
        .map_err(cudf_to_df)?;

    Ok(Some(batch))
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
fn batches_to_table(batches: &[RecordBatch]) -> Result<CuDFTable, libcudf_rs::CuDFError> {
    let views: Vec<CuDFTableView> = batches
        .iter()
        .map(CuDFTableView::from_record_batch)
        .collect::<Result<_, _>>()?;
    CuDFTable::concat(views)
}

/// Try to convert a `HashJoinExec` to GPU. Returns `None` for unsupported
/// configurations: non-column keys, non-equi filters, unsupported join types.
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

    match node.join_type() {
        JoinType::Inner | JoinType::Left | JoinType::Full => {}
        _ => return Ok(None),
    }

    if node.null_equality() != NullEquality::NullEqualsNothing {
        return Ok(None);
    }

    if node.filter().is_some() {
        return Ok(None);
    }

    Ok(Some(Arc::new(CuDFHashJoinExec::from_hash_join_exec(node)?)))
}

#[cfg(test)]
mod test {
    use super::{try_as_cudf_hash_join, CuDFHashJoinExec};
    use crate::physical::{CuDFLoadExec, CuDFUnloadExec};
    use arrow::array::record_batch;
    use arrow::array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::common::{JoinSide, JoinType, NullEquality};
    use datafusion::execution::TaskContext;
    use datafusion_physical_plan::expressions::Column;
    use datafusion_physical_plan::joins::utils::{ColumnIndex, JoinFilter};
    use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion_physical_plan::test::TestMemoryExec;
    use datafusion_physical_plan::{ExecutionPlan, PhysicalExpr};
    use futures_util::TryStreamExt;
    use std::error::Error;
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
        let ls = left.schema();
        let rs = right_batches
            .first()
            .expect("right side should have at least one batch")
            .schema();
        // Both sides go through CuDFLoadExec — symmetric GPU upload.
        let left_in = Arc::new(CuDFLoadExec::try_new(Arc::new(TestMemoryExec::try_new(
            &[vec![left]],
            ls.clone(),
            None,
        )?))?);
        let right_in = Arc::new(CuDFLoadExec::try_new(Arc::new(TestMemoryExec::try_new(
            &[right_batches],
            rs.clone(),
            None,
        )?))?);
        let on = vec![(
            Arc::new(Column::new("key", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("key", 0)) as Arc<dyn PhysicalExpr>,
        )];
        let exec =
            CuDFHashJoinExec::try_new(left_in, right_in, on, join_type, None, partition_mode)?;
        let unload = CuDFUnloadExec::new(Arc::new(exec));
        let stream = unload.execute(0, Arc::new(TaskContext::default()))?;
        Ok(stream.try_collect::<Vec<_>>().await?)
    }

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
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
        let schema = right_batch().schema();
        let right_a = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![2])),
                Arc::new(Int32Array::from(vec![200])),
            ],
        )?;
        let right_b = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![3])),
                Arc::new(Int32Array::from(vec![300])),
            ],
        )?;

        let out = run_join_with_right_batches(
            left_batch(),
            vec![right_a, right_b],
            JoinType::Inner,
            PartitionMode::CollectLeft,
        )
        .await?;

        assert_eq!(out.len(), 2);
        assert_eq!(total_rows(&out), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_left_join_no_right_batches() -> Result<(), Box<dyn Error>> {
        // Right partition produces zero batches.
        // Left/Full joins must still return all left rows with nulls in right columns.
        let ls = left_batch().schema();
        let rs = right_batch().schema();
        let left_in = Arc::new(CuDFLoadExec::try_new(Arc::new(TestMemoryExec::try_new(
            &[vec![left_batch()]],
            ls,
            None,
        )?))?);
        let right_in = Arc::new(CuDFLoadExec::try_new(Arc::new(TestMemoryExec::try_new(
            &[vec![]],
            rs,
            None,
        )?))?);
        let on = vec![(
            Arc::new(Column::new("key", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("key", 0)) as Arc<dyn PhysicalExpr>,
        )];
        let exec = CuDFHashJoinExec::try_new(
            left_in,
            right_in,
            on,
            JoinType::Left,
            None,
            PartitionMode::CollectLeft,
        )?;
        let unload = CuDFUnloadExec::new(Arc::new(exec));
        let stream = unload.execute(0, Arc::new(TaskContext::default()))?;
        let out: Vec<RecordBatch> = stream.try_collect().await?;
        // All 4 left rows preserved; right columns are null.
        assert_eq!(total_rows(&out), 4);
        assert_eq!(out[0].num_columns(), 4);
        Ok(())
    }

    #[tokio::test]
    async fn test_inner_join_partitioned() -> Result<(), Box<dyn Error>> {
        // Partitioned mode builds the left table per-partition rather than once globally.
        let out = run_join(
            left_batch(),
            right_batch(),
            JoinType::Inner,
            PartitionMode::Partitioned,
        )
        .await?;
        assert_eq!(total_rows(&out), 2); // keys 2 and 3 match
        assert_eq!(out[0].num_columns(), 4);
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
    fn test_join_filter_bails_to_cpu() -> Result<(), Box<dyn Error>> {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
        let left = Arc::new(TestMemoryExec::try_new(&[], schema.clone(), None)?);
        let right = Arc::new(TestMemoryExec::try_new(&[], schema.clone(), None)?);
        let on = vec![(
            Arc::new(Column::new("key", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("key", 0)) as Arc<dyn PhysicalExpr>,
        )];
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
            on,
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
    async fn test_full_join() -> Result<(), Box<dyn Error>> {
        check_correct(
            r#"SELECT a."MinTemp", b."MaxTemp" FROM weather a
               FULL JOIN weather b ON a."MinTemp" = b."MinTemp"
               ORDER BY a."MinTemp", b."MaxTemp" LIMIT 10"#,
        )
        .await
    }
}
