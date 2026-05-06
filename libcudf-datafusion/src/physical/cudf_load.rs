use crate::errors::cudf_to_df;
use crate::metrics::CuDFBaselineMetrics;
use crate::planner::CuDFConfig;
use crate::task_context::{cuda_streams_enabled, CuDFTaskContext};
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
use libcudf_rs::{
    is_cudf_array, pin_record_batch, synchronize_default_stream, CuDFStream, CuDFTable,
};
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

#[derive(Debug)]
pub struct CuDFLoadExec {
    input: Arc<dyn ExecutionPlan>,
    /// GPU segment id this load is the bottom of. Used to look up the CUDA
    /// stream for `(segment_id, partition)` when streams are enabled.
    /// `0` is the "use default stream" sentinel — the planner only
    /// assigns nonzero ids to segments on the streams allowlist.
    segment_id: usize,
    /// When true, output partition `p` uploads input partition `p`.
    /// When false, output partition 0 uploads every input partition into one stream.
    preserve_input_partitioning: bool,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl CuDFLoadExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>) -> Result<Self, DataFusionError> {
        Self::try_new_with_segment_id_and_partitioning(input, 0, false)
    }

    pub fn try_new_preserving_partitioning(
        input: Arc<dyn ExecutionPlan>,
        preserve_input_partitioning: bool,
    ) -> Result<Self, DataFusionError> {
        Self::try_new_with_segment_id_and_partitioning(input, 0, preserve_input_partitioning)
    }

    pub fn try_new_with_segment_id(
        input: Arc<dyn ExecutionPlan>,
        segment_id: usize,
    ) -> Result<Self, DataFusionError> {
        Self::try_new_with_segment_id_and_partitioning(input, segment_id, false)
    }

    fn try_new_with_segment_id_and_partitioning(
        input: Arc<dyn ExecutionPlan>,
        segment_id: usize,
        preserve_input_partitioning: bool,
    ) -> Result<Self, DataFusionError> {
        let output_partitioning = if preserve_input_partitioning {
            input.output_partitioning().clone()
        } else {
            Partitioning::UnknownPartitioning(1)
        };
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(cudf_schema_compatibility_map(input.schema())),
            output_partitioning,
            input.properties().emission_type,
            input.properties().boundedness,
        ));
        Ok(Self {
            input,
            segment_id,
            preserve_input_partitioning,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    pub fn segment_id(&self) -> usize {
        self.segment_id
    }

    pub(crate) fn with_segment_id(&self, segment_id: usize) -> Self {
        Self {
            input: Arc::clone(&self.input),
            segment_id,
            preserve_input_partitioning: self.preserve_input_partitioning,
            properties: Arc::clone(&self.properties),
            metrics: self.metrics.clone(),
        }
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
        Ok(Arc::new(Self::try_new_with_segment_id_and_partitioning(
            input,
            self.segment_id,
            self.preserve_input_partitioning,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let input_partitions = self.input.output_partitioning().partition_count();
        if self.preserve_input_partitioning {
            if partition >= input_partitions {
                return exec_err!(
                    "CuDFLoadExec invalid partition {partition}; input has {input_partitions} partitions"
                );
            }
        } else {
            assert_eq_or_internal_err!(partition, 0, "CuDFLoadExec invalid partition {partition}");
        }

        let pinned_input = context
            .session_config()
            .options()
            .extensions
            .get::<CuDFConfig>()
            .is_none_or(|cfg| cfg.pinned_input);

        // If streams are enabled AND this segment was tagged as
        // stream-eligible by the planner (segment_id != 0), allocate (or
        // reuse) the stream for this (segment, output partition=0) pair.
        // The CuDFTaskContext extension owns the mapping; downstream
        // CuDF operators in the same segment look up the same stream so
        // they all run on it. `segment_id == 0` is the "ineligible / use
        // default stream" sentinel — see `assign_segment_ids` in the
        // planner.
        let cuda_stream = if cuda_streams_enabled(&context) && self.segment_id != 0 {
            let cudf_cfg = CuDFConfig::from_config_options(context.session_config().options())?;
            let cudf_ctx = CuDFTaskContext::from_ctx(&context)?;
            let segment_id = self.segment_id;
            let stream = cudf_ctx.stream(segment_id, partition).unwrap_or_else(|| {
                let stream = cudf_cfg.stream_source().allocate();
                cudf_ctx.set_stream(segment_id, partition, Arc::clone(&stream));
                stream
            });
            Some(stream)
        } else {
            None
        };

        // use a stream that allows each sender to put in at
        // least one result in an attempt to maximize
        // parallelism.
        let output_streams = if self.preserve_input_partitioning {
            1
        } else {
            input_partitions
        };
        let mut builder = CuDFRecordBatchReceiverStreamBuilder {
            inner: RecordBatchReceiverStream::builder(self.schema(), output_streams),
            ctx: CuDFRecordBatchReceiverStreamBuilderCtx {
                schema: self.schema(),
                metrics: CuDFLoadMetrics::new(&self.metrics, partition),
                cuda_stream,
                pinned_input,
            },
        };

        // spawn independent tasks whose resulting streams (of batches)
        // are sent to the channel for consumption.
        let input_range: Box<dyn Iterator<Item = usize>> = if self.preserve_input_partitioning {
            Box::new(std::iter::once(partition))
        } else {
            Box::new(0..input_partitions)
        };
        for part_i in input_range {
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
    /// Per-segment CUDA stream when streams are enabled and the segment
    /// is stream-eligible. `None` means use the default stream.
    cuda_stream: Option<Arc<CuDFStream>>,
    /// Stage host batches through pinned (page-locked) memory before the
    /// H2D copy. Faster `cudaMemcpyAsync` because no pageable-staging
    /// step in the driver.
    pinned_input: bool,
}

impl CuDFRecordBatchReceiverStreamBuilder {
    fn run_input(&mut self, mut host_stream: SendableRecordBatchStream) {
        let ctx = self.ctx.clone();
        let output = self.inner.tx();
        self.inner.spawn(async move {
            loop {
                let ctx = ctx.clone();
                let input_wait_timer = ctx.metrics.input_wait_time.timer();
                let batch_or_err = host_stream.next().await;
                input_wait_timer.done();

                let Some(batch_or_err) = batch_or_err else {
                    break;
                };

                let compute_timer = ctx.metrics.baseline.elapsed_compute().timer();
                let batch = match batch_or_err {
                    Ok(batch) => {
                        let cast_timer = ctx.metrics.cast_time.timer();
                        let batch = cast_to_target_schema(batch, Arc::clone(&ctx.schema))?;
                        cast_timer.done();
                        batch
                    }
                    Err(err) => return Err(err),
                };

                if batch.columns().iter().any(|c| is_cudf_array(c)) {
                    return exec_err!(
                        "Cannot move RecordBatch from host to CuDF: a column is already a CuDF array"
                    );
                }
                ctx.metrics.record_input(&batch);
                let schema = batch.schema();
                // When `pinned_input` is enabled, stage the host batch through
                // pinned (page-locked) memory so the upload is a direct DMA
                // without the driver's pageable-staging step. The pinned source
                // must outlive the async copy, so the relevant stream is
                // synchronized before the pinned batch is dropped at the end of
                // this closure.
                let table = if ctx.pinned_input {
                    let pin_timer = ctx.metrics.pin_time.timer();
                    let pinned = pin_record_batch(batch).map_err(cudf_to_df)?;
                    pin_timer.done();

                    let import_timer = ctx.metrics.import_time.timer();
                    let table = match ctx.cuda_stream.as_deref() {
                        Some(stream) => CuDFTable::from_arrow_host_on(pinned.clone(), stream)
                            .map_err(cudf_to_df)?,
                        None => CuDFTable::from_arrow_host(pinned.clone())
                            .map_err(cudf_to_df)?,
                    };
                    import_timer.done();

                    let sync_timer = ctx.metrics.sync_time.timer();
                    match ctx.cuda_stream.as_deref() {
                        Some(stream) => stream.synchronize().map_err(cudf_to_df)?,
                        None => synchronize_default_stream().map_err(cudf_to_df)?,
                    }
                    sync_timer.done();
                    drop(pinned);
                    table
                } else {
                    let import_timer = ctx.metrics.import_time.timer();
                    let table = match ctx.cuda_stream.as_deref() {
                        Some(stream) => CuDFTable::from_arrow_host_on(batch, stream)
                            .map_err(cudf_to_df)?,
                        None => CuDFTable::from_arrow_host(batch).map_err(cudf_to_df)?,
                    };
                    import_timer.done();
                    table
                };
                let output_batch_timer = ctx.metrics.output_batch_time.timer();
                let num_rows = table.num_rows();
                let cudf_cols: Vec<Arc<dyn Array>> = table
                    .into_columns()
                    .into_iter()
                    .map(|c| Arc::new(c.into_view()) as Arc<dyn Array>)
                    .collect();
                let batch = libcudf_rs::record_batch_with_schema(cudf_cols, &schema, num_rows)?;
                output_batch_timer.done();

                ctx.metrics.baseline.record_output(&batch);
                compute_timer.done();

                let send_timer = ctx.metrics.output_send_time.timer();
                let send_result = output.send(Ok(batch)).await;
                send_timer.done();
                if send_result.is_err() {
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
