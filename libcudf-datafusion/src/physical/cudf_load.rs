use crate::errors::cudf_to_df;
use crate::metrics::CuDFBaselineMetrics;
use crate::planner::CuDFConfig;
use arrow::array::{Array, RecordBatch, RecordBatchOptions};
use arrow_schema::{ArrowError, DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::{assert_eq_or_internal_err, exec_err, plan_err, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_expr_common::metrics::MetricsSet;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_plan::stream::{
    RecordBatchReceiverStream, RecordBatchReceiverStreamBuilder,
};
use datafusion_physical_plan::{
    internal_err, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties,
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
        // CoalescePartitionsExec produces a single partition
        let pinned_input = context
            .session_config()
            .options()
            .extensions
            .get::<CuDFConfig>()
            .map_or(true, |cfg| cfg.pinned_input);

        assert_eq_or_internal_err!(partition, 0, "CuDFLoadExec invalid partition {partition}");

        let input_partitions = self.input.output_partitioning().partition_count();

        // use a stream that allows each sender to put in at
        // least one result in an attempt to maximize
        // parallelism.
        let mut builder = CuDFRecordBatchReceiverStreamBuilder {
            inner: RecordBatchReceiverStream::builder(self.schema(), input_partitions),
            ctx: CuDFRecordBatchReceiverStreamBuilderCtx {
                schema: self.schema(),
                metrics: CuDFBaselineMetrics::new(&self.metrics, partition),
                pinned_input,
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
    metrics: CuDFBaselineMetrics,
    pinned_input: bool,
}

impl CuDFRecordBatchReceiverStreamBuilder {
    fn run_input(&mut self, mut host_stream: SendableRecordBatchStream) {
        let ctx = self.ctx.clone();
        let output = self.inner.tx();
        self.inner.spawn(async move {
            // Transfer batches from inner stream to the output tx
            // immediately.
            while let Some(batch_or_err) = host_stream.next().await {
                let ctx = ctx.clone();
                let task = SpawnedTask::spawn_blocking(move || {
                    let _timer_guard = ctx.metrics.elapsed_compute().timer();
                    let batch = match batch_or_err {
                        Ok(batch) => cast_to_target_schema(batch, Arc::clone(&ctx.schema))?,
                        Err(err) => return Err(err),
                    };

                    if batch.columns().iter().any(|c| is_cudf_array(c)) {
                        return exec_err!("Cannot move RecordBatch from host to CuDF: a column is already a CuDF array");
                    }
                    let schema = batch.schema();
                    // When `pinned_input` is enabled, stage the host batch through
                    // pinned (page-locked) memory so the upload is a direct DMA
                    // without the driver's pageable-staging step. The pinned source
                    // must outlive the async copy, so the default stream is
                    // synchronized before the pinned batch is dropped at the end of
                    // this closure.
                    //
                    // TODO(memory-tracking): the bytes we pin here are not registered
                    // against DataFusion's `MemoryPool`, so they don't show up in
                    // `EXPLAIN ANALYZE` and won't trigger backpressure if a query has
                    // a memory cap. The OS-level `RLIMIT_MEMLOCK` / `cudaMallocHost`
                    // failure path is the current safety net. Worth wiring through a
                    // `MemoryReservation` (try_grow / shrink per batch) if someone
                    // starts configuring per-query memory caps for cuDF operators.
                    let table = if ctx.pinned_input {
                        let pinned_batch = pin_record_batch(batch).map_err(cudf_to_df)?;
                        let table =
                            CuDFTable::from_arrow_host(pinned_batch).map_err(cudf_to_df)?;
                        synchronize_default_stream().map_err(cudf_to_df)?;
                        table
                    } else {
                        CuDFTable::from_arrow_host(batch).map_err(cudf_to_df)?
                    };
                    let num_rows = table.num_rows();
                    let cudf_cols: Vec<Arc<dyn Array>> = table
                        .into_columns()
                        .into_iter()
                        .map(|c| Arc::new(c.into_view()) as Arc<dyn Array>)
                        .collect();
                    let batch =
                        libcudf_rs::record_batch_with_schema(cudf_cols, &schema, num_rows)?;
                    ctx.metrics.record_output(&batch);
                    Ok(batch)
                });
                let send_result = match task.await {
                    Ok(result) => output.send(result).await,
                    Err(err) => output.send(internal_err!("{err}")).await,
                };
                if send_result.is_err() {
                    break
                }
            }

            Ok(())
        });
    }

    fn build(self) -> SendableRecordBatchStream {
        self.inner.build()
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
