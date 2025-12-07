use crate::errors::cudf_to_df;
use arrow::array::{Array, RecordBatch};
use arrow_schema::{ArrowError, DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::common::{exec_err, plan_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures_util::stream::StreamExt;
use libcudf_rs::{is_cudf_array, CuDFColumn};
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

#[derive(Debug)]
pub struct CuDFLoadExec {
    input: Arc<dyn ExecutionPlan>,
}

impl CuDFLoadExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        Self { input }
    }
}

impl DisplayAs for CuDFLoadExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CudfLoadExec")
    }
}

impl ExecutionPlan for CuDFLoadExec {
    fn name(&self) -> &str {
        "CuDFLoadExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.input.properties()
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
        Ok(Arc::new(Self { input }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let host_stream = self.input.execute(partition, context)?;
        let schema = cudf_schema_compatibility_map(host_stream.schema());
        let target_schema = Arc::clone(&schema);

        let cudf_stream = host_stream.map(move |batch_or_err| {
            let batch = match batch_or_err {
                Ok(batch) => cudf_batch_compatibility_map(batch, Arc::clone(&target_schema))?,
                Err(err) => return Err(err),
            };

            let original_cols = batch.columns();
            let mut cudf_cols: Vec<Arc<dyn Array>> = Vec::with_capacity(original_cols.len());
            for original_col in original_cols {
                if is_cudf_array(original_col) {
                    return exec_err!(
                        "Cannot move RecordBatch from host to CuDF: a column is already a CuDF array"
                    );
                }
                let col = CuDFColumn::from_arrow_host(original_col).map_err(cudf_to_df)?;
                cudf_cols.push(Arc::new(col.into_view()));
            }

            Ok(RecordBatch::try_new(batch.schema(), cudf_cols)?)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, cudf_stream)))
    }
}

fn cudf_schema_compatibility_map(schema: SchemaRef) -> SchemaRef {
    let mut new_fields = Vec::with_capacity(schema.fields.len());

    for field in schema.fields() {
        if field.data_type() == &DataType::Utf8View {
            new_fields.push(FieldRef::new(Field::new(
                field.name(),
                DataType::Utf8,
                field.is_nullable(),
            )));
        } else {
            new_fields.push(Arc::clone(field));
        }
    }

    SchemaRef::new(Schema::new(new_fields))
}

fn cudf_batch_compatibility_map(
    batch: RecordBatch,
    target_schema: SchemaRef,
) -> Result<RecordBatch, ArrowError> {
    let mut new_cols = Vec::with_capacity(batch.num_columns());

    let original_schema = batch.schema();
    let original_fields = original_schema.fields();
    let target_fields = target_schema.fields();

    for ((col, original_field), target_field) in batch
        .columns()
        .iter()
        .zip(original_fields)
        .zip(target_fields)
    {
        // CuDF does not support UTF8View, so we cast it to UTF8.
        if original_field.data_type() != target_field.data_type() {
            new_cols.push(arrow::compute::cast(&col, target_field.data_type())?);
        } else {
            new_cols.push(Arc::clone(col));
        }
    }

    RecordBatch::try_new(target_schema, new_cols)
}
