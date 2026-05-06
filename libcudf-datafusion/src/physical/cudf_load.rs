use crate::errors::cudf_to_df;
use crate::metrics::CuDFBaselineMetrics;
use crate::planner::CuDFConfig;
use arrow::array::{Array, RecordBatch, RecordBatchOptions};
use arrow_schema::{ArrowError, DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::common::{exec_err, plan_err, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr_common::metrics::MetricsSet;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
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
            input.properties().partitioning.clone(),
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
        let pinned_input = context
            .session_config()
            .options()
            .extensions
            .get::<CuDFConfig>()
            .is_none_or(|cfg| cfg.pinned_input);
        let host_stream = self.input.execute(partition, context)?;
        let target_schema = self.schema();

        let metrics = CuDFBaselineMetrics::new(&self.metrics, partition);

        let cudf_stream = host_stream.map(move |batch_or_err| {
            let _timer_guard = metrics.elapsed_compute().timer();
            let batch = match batch_or_err {
                Ok(batch) => cast_to_target_schema(batch, Arc::clone(&target_schema))?,
                Err(err) => return Err(err),
            };

            if batch.columns().iter().any(|c| is_cudf_array(c)) {
                return exec_err!(
                    "Cannot move RecordBatch from host to CuDF: a column is already a CuDF array"
                );
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
            let table = if pinned_input {
                let pinned_batch = pin_record_batch(batch).map_err(cudf_to_df)?;
                let table = CuDFTable::from_arrow_host(pinned_batch).map_err(cudf_to_df)?;
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
            let batch = libcudf_rs::record_batch_with_schema(cudf_cols, &schema, num_rows)?;
            metrics.record_output(&batch);
            Ok(batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            cudf_stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
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
