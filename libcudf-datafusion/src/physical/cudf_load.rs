use crate::errors::cudf_to_df;
use crate::metrics::CuDFBaselineMetrics;
use crate::planner::CuDFConfig;
use arrow::array::{Array, RecordBatch, RecordBatchOptions};
use arrow_schema::{ArrowError, DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::common::{assert_eq_or_internal_err, exec_err, plan_err, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_expr_common::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time,
};
use datafusion_physical_plan::stream::{
    RecordBatchReceiverStream, RecordBatchReceiverStreamBuilder,
};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use futures_util::stream::StreamExt;
use libcudf_rs::{is_cudf_array, pin_record_batch, synchronize_default_stream, CuDFTable};
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

#[derive(Debug)]
pub struct CuDFLoadExec {
    input: Arc<dyn ExecutionPlan>,

    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl CuDFLoadExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>) -> Result<Self, DataFusionError> {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(cudf_schema_compatibility_map(input.schema())),
            Partitioning::UnknownPartitioning(1),
            input.properties().emission_type,
            input.properties().boundedness,
        ));
        Ok(Self {
            input,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl DisplayAs for CuDFLoadExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CuDFLoadExec")
    }
}

impl ExecutionPlan for CuDFLoadExec {
    fn name(&self) -> &str {
        "CuDFLoadExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return plan_err!(
                "CuDFLoadExec expects exactly 1 child, {} where provided",
                children.len()
            );
        }
        let input = Arc::clone(&children[0]);
        Ok(Arc::new(Self::try_new(input)?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        assert_eq_or_internal_err!(partition, 0, "CuDFLoadExec invalid partition {partition}");

        let input_partitions = self.input.output_partitioning().partition_count();
        let cudf_cfg = CuDFConfig::from_config_options(context.session_config().options())?;
        let coalesce_target_rows = cudf_cfg.batch_size;

        // use a stream that allows each sender to put in at
        // least one result in an attempt to maximize
        // parallelism.
        let mut builder = CuDFRecordBatchReceiverStreamBuilder {
            inner: RecordBatchReceiverStream::builder(self.schema(), input_partitions),
            ctx: CuDFRecordBatchReceiverStreamBuilderCtx {
                schema: self.schema(),
                metrics: CuDFLoadMetrics::new(&self.metrics, partition),
                coalesce_target_rows,
            },
        };

        // spawn independent tasks whose resulting streams (of batches)
        // are sent to the channel for consumption.
        for part_i in 0..input_partitions {
            let input = Arc::clone(&self.input);
            let host_stream = input.execute(part_i, context.clone())?;
            builder.run_input(host_stream);
        }

        Ok(builder.build())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

struct CuDFRecordBatchReceiverStreamBuilder {
    inner: RecordBatchReceiverStreamBuilder,
    ctx: CuDFRecordBatchReceiverStreamBuilderCtx,
}

#[derive(Clone)]
struct CuDFRecordBatchReceiverStreamBuilderCtx {
    schema: SchemaRef,
    metrics: CuDFLoadMetrics,
    coalesce_target_rows: usize,
}

impl CuDFRecordBatchReceiverStreamBuilder {
    fn run_input(&mut self, mut host_stream: SendableRecordBatchStream) {
        let ctx = self.ctx.clone();
        let output = self.inner.tx();
        self.inner.spawn(async move {
            // Coalesce small upstream batches per partition before uploading.
            // Downstream cuDF operators have a ~300µs per-batch kernel-launch
            // overhead, so without consolidation small upstream batches end up
            // dominated by launch cost. Target row count is configurable via
            // `CuDFConfig::load_coalesce_target_rows`.
            let mut coalescer = arrow::compute::BatchCoalescer::new(
                Arc::clone(&ctx.schema),
                ctx.coalesce_target_rows,
            );
            loop {
                let input_wait_timer = ctx.metrics.input_wait_time.timer();
                let batch_or_err = host_stream.next().await;
                input_wait_timer.done();

                let upstream_done = batch_or_err.is_none();
                let compute_timer = ctx.metrics.baseline.elapsed_compute().timer();

                match batch_or_err {
                    Some(Ok(batch)) => {
                        let cast_timer = ctx.metrics.cast_time.timer();
                        let batch = cast_to_target_schema(batch, Arc::clone(&ctx.schema))?;
                        cast_timer.done();

                        if batch.columns().iter().any(|c| is_cudf_array(c)) {
                            return exec_err!(
                                "Cannot move RecordBatch from host to CuDF: a column is already a CuDF array"
                            );
                        }
                        ctx.metrics.record_input(&batch);
                        let coalesce_timer = ctx.metrics.coalesce_time.timer();
                        coalescer.push_batch(batch).map_err(|e| {
                            DataFusionError::ArrowError(Box::new(e), None)
                        })?;
                        coalesce_timer.done();
                    }
                    Some(Err(err)) => return Err(err),
                    None => {
                        let coalesce_timer = ctx.metrics.coalesce_time.timer();
                        coalescer.finish_buffered_batch().map_err(|e| {
                            DataFusionError::ArrowError(Box::new(e), None)
                        })?;
                        coalesce_timer.done();
                    }
                }

                let mut ready: Vec<RecordBatch> = Vec::new();
                loop {
                    let coalesce_timer = ctx.metrics.coalesce_time.timer();
                    let coalesced = coalescer.next_completed_batch();
                    coalesce_timer.done();
                    let Some(coalesced) = coalesced else {
                        break;
                    };
                    let schema = coalesced.schema();

                    let pin_timer = ctx.metrics.pin_time.timer();
                    let pinned_batch = pin_record_batch(coalesced).map_err(cudf_to_df)?;
                    pin_timer.done();

                    let import_timer = ctx.metrics.import_time.timer();
                    let table = CuDFTable::from_arrow_host(pinned_batch).map_err(cudf_to_df)?;
                    import_timer.done();

                    let sync_timer = ctx.metrics.sync_time.timer();
                    synchronize_default_stream().map_err(cudf_to_df)?;
                    sync_timer.done();

                    let output_batch_timer = ctx.metrics.output_batch_time.timer();
                    let num_rows = table.num_rows();
                    let cudf_cols: Vec<Arc<dyn Array>> = table
                        .into_columns()
                        .into_iter()
                        .map(|c| Arc::new(c.into_view()) as Arc<dyn Array>)
                        .collect();
                    let batch =
                        libcudf_rs::record_batch_with_schema(cudf_cols, &schema, num_rows)?;
                    output_batch_timer.done();

                    ctx.metrics.baseline.record_output(&batch);
                    ready.push(batch);
                }

                // Stop the compute timer before the bounded-channel awaits;
                // sending out a backpressured batch is wait time, not compute.
                compute_timer.done();

                for batch in ready {
                    let send_timer = ctx.metrics.output_send_time.timer();
                    let send_result = output.send(Ok(batch)).await;
                    send_timer.done();
                    if send_result.is_err() {
                        return Ok(());
                    }
                }

                if upstream_done {
                    break;
                }
            }

            Ok(())
        });
    }

    fn build(self) -> SendableRecordBatchStream {
        self.inner.build()
    }
}

#[derive(Clone)]
struct CuDFLoadMetrics {
    baseline: CuDFBaselineMetrics,
    input_wait_time: Time,
    cast_time: Time,
    coalesce_time: Time,
    pin_time: Time,
    import_time: Time,
    sync_time: Time,
    output_batch_time: Time,
    output_send_time: Time,
    input_batches: Count,
    input_rows: Count,
    input_bytes: Count,
    input_columns: Count,
    bool_columns: Count,
    numeric_columns: Count,
    decimal_columns: Count,
    string_columns: Count,
    temporal_columns: Count,
    other_columns: Count,
}

impl CuDFLoadMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline: CuDFBaselineMetrics::new(metrics, partition),
            input_wait_time: MetricBuilder::new(metrics).subset_time("input_wait_time", partition),
            cast_time: MetricBuilder::new(metrics).subset_time("cast_time", partition),
            coalesce_time: MetricBuilder::new(metrics).subset_time("coalesce_time", partition),
            pin_time: MetricBuilder::new(metrics).subset_time("pin_time", partition),
            import_time: MetricBuilder::new(metrics).subset_time("import_time", partition),
            sync_time: MetricBuilder::new(metrics).subset_time("sync_time", partition),
            output_batch_time: MetricBuilder::new(metrics)
                .subset_time("output_batch_time", partition),
            output_send_time: MetricBuilder::new(metrics)
                .subset_time("output_send_time", partition),
            input_batches: MetricBuilder::new(metrics).counter("input_batches", partition),
            input_rows: MetricBuilder::new(metrics).counter("input_rows", partition),
            input_bytes: MetricBuilder::new(metrics).counter("input_bytes", partition),
            input_columns: MetricBuilder::new(metrics).counter("input_columns", partition),
            bool_columns: MetricBuilder::new(metrics).counter("bool_columns", partition),
            numeric_columns: MetricBuilder::new(metrics).counter("numeric_columns", partition),
            decimal_columns: MetricBuilder::new(metrics).counter("decimal_columns", partition),
            string_columns: MetricBuilder::new(metrics).counter("string_columns", partition),
            temporal_columns: MetricBuilder::new(metrics).counter("temporal_columns", partition),
            other_columns: MetricBuilder::new(metrics).counter("other_columns", partition),
        }
    }

    fn record_input(&self, batch: &RecordBatch) {
        self.input_batches.add(1);
        self.input_rows.add(batch.num_rows());
        self.input_columns.add(batch.num_columns());
        self.input_bytes.add(
            batch
                .columns()
                .iter()
                .map(|col| col.get_array_memory_size())
                .sum::<usize>(),
        );

        for field in batch.schema().fields() {
            match field.data_type() {
                DataType::Boolean => self.bool_columns.add(1),
                DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float16
                | DataType::Float32
                | DataType::Float64 => self.numeric_columns.add(1),
                DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _) => self.decimal_columns.add(1),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                    self.string_columns.add(1)
                }
                DataType::Date32
                | DataType::Date64
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Timestamp(_, _)
                | DataType::Duration(_)
                | DataType::Interval(_) => self.temporal_columns.add(1),
                _ => self.other_columns.add(1),
            }
        }
    }
}

/// Converts Arrow scalar types that cuDF does not support into cuDF-compatible equivalents.
pub(crate) fn normalize_scalar_for_cudf(value: ScalarValue) -> ScalarValue {
    match value {
        ScalarValue::Utf8View(s) => ScalarValue::Utf8(s),
        other => other,
    }
}

/// Maps an Arrow schema to cuDF-compatible types (`Utf8View -> Utf8`).
pub(crate) fn cudf_schema_compatibility_map(schema: SchemaRef) -> SchemaRef {
    let mut new_fields = Vec::with_capacity(schema.fields.len());

    for field in schema.fields() {
        let field = match field.data_type() {
            DataType::Utf8View => FieldRef::new(Field::new(
                field.name(),
                DataType::Utf8,
                field.is_nullable(),
            )),
            _ => Arc::clone(field),
        };
        new_fields.push(field);
    }

    SchemaRef::new(Schema::new(new_fields))
}

pub(crate) fn cast_to_target_schema(
    batch: RecordBatch,
    target_schema: SchemaRef,
) -> Result<RecordBatch, ArrowError> {
    let num_rows = batch.num_rows();
    let columns = batch
        .columns()
        .iter()
        .zip(target_schema.fields())
        .map(|(col, field)| arrow::compute::cast(col, field.data_type()))
        .collect::<Result<Vec<_>, _>>()?;

    let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
    RecordBatch::try_new_with_options(target_schema, columns, &options)
}
