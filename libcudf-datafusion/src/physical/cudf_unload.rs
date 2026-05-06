use crate::errors::cudf_to_df;
use crate::metrics::CuDFBaselineMetrics;
use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::common::{plan_err, DataFusionError};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr_common::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures_util::StreamExt;
use libcudf_rs::CuDFTableView;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

#[derive(Debug)]
pub struct CuDFUnloadExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl CuDFUnloadExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let mut properties = input.properties().as_ref().clone();
        properties.eq_properties =
            EquivalenceProperties::new(cudf_unload_schema_map(input.schema()));
        Self {
            properties: Arc::new(properties),
            input,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    pub fn with_target_schema(&self, target_schema: SchemaRef) -> Self {
        let mut properties = self.properties.as_ref().clone();
        properties.eq_properties = EquivalenceProperties::new(target_schema);
        Self {
            properties: Arc::new(properties),
            input: Arc::clone(&self.input),
            metrics: self.metrics.clone(),
        }
    }
}

impl DisplayAs for CuDFUnloadExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CuDFUnloadExec")
    }
}

impl ExecutionPlan for CuDFUnloadExec {
    fn name(&self) -> &str {
        "CuDFUnloadExec"
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
                "CuDFUnloadExec expects exactly 1 child, {} where provided",
                children.len()
            );
        }
        let input = Arc::clone(&children[0]);
        Ok(Arc::new(Self::new(input)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let cudf_stream = self.input.execute(partition, context)?;

        let metrics = CuDFBaselineMetrics::new(&self.metrics, partition);

        let target_schema = self.schema();
        let host_stream = cudf_stream.map(move |batch_or_err| {
            let _timer_guard = metrics.elapsed_compute().timer();
            let batch = match batch_or_err {
                Ok(batch) => batch,
                Err(err) => return Err(err),
            };

            let view = CuDFTableView::from_record_batch(&batch).map_err(cudf_to_df)?;
            let host_batch = view.to_arrow_host().map_err(cudf_to_df)?;
            let num_rows = host_batch.num_rows();
            let columns = host_batch
                .columns()
                .iter()
                .zip(target_schema.fields())
                .map(|(col, field)| arrow::compute::cast(col, field.data_type()))
                .collect::<Result<Vec<_>, _>>()?;
            let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
            let batch = RecordBatch::try_new_with_options(target_schema.clone(), columns, &options)
                .map_err(|err| {
                    DataFusionError::ArrowError(
                        Box::new(err),
                        Some("Error while unloading a RecordBatch from CuDF into host".to_string()),
                    )
                })?;
            metrics.record_output(&batch);
            Ok(batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            host_stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// Partial reverse of [`cudf_schema_compatibility_map`]: restores `Utf8 -> Utf8View` so
/// downstream CPU nodes see the types they expect from the original parquet schema.
///
/// Only Utf8View is reversed, decimal precision normalization is not because the original
/// precision is not recoverable.
fn cudf_unload_schema_map(schema: SchemaRef) -> SchemaRef {
    let new_fields: Vec<FieldRef> = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Utf8 => FieldRef::new(Field::new(
                field.name(),
                DataType::Utf8View,
                field.is_nullable(),
            )),
            _ => Arc::clone(field),
        })
        .collect();
    SchemaRef::new(Schema::new(new_fields))
}

#[cfg(test)]
mod tests {
    use super::CuDFUnloadExec;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_physical_plan::{test::TestMemoryExec, ExecutionPlan};
    use std::sync::Arc;

    #[test]
    fn test_schema_restores_utf8view() {
        // Input schema uses Utf8 (cuDF's normalised string type).
        // CuDFUnloadExec must restore Utf8View so downstream CPU nodes see the
        // type they expect from the original parquet schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
        ]));
        let input = Arc::new(TestMemoryExec::try_new(&[], schema, None).unwrap());
        let unload = CuDFUnloadExec::new(input);
        let out = unload.schema();
        assert_eq!(out.field(0).data_type(), &DataType::Utf8View);
        assert_eq!(out.field(1).data_type(), &DataType::Int32);
    }
}
