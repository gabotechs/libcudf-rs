use crate::errors::cudf_to_df;
use arrow::array::{Array, RecordBatch};
use arrow_schema::{ArrowError, DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::common::{exec_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    execute_stream, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures_util::stream::StreamExt;
use libcudf_rs::{is_cudf_array, CuDFColumn};
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

#[derive(Debug)]
pub struct CuDFLoadExec {
    input: Arc<dyn ExecutionPlan>,

    properties: PlanProperties,
}

impl CuDFLoadExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let mut properties = input.properties().clone();
        properties.partitioning = Partitioning::UnknownPartitioning(1);
        Self { input, properties }
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

    fn properties(&self) -> &PlanProperties {
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
        Ok(Arc::new(Self::new(input)))
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let host_stream = execute_stream(Arc::clone(&self.input), context)?;
        let schema = cudf_schema_compatibility_map(host_stream.schema());
        let target_schema = Arc::clone(&schema);

        let cudf_stream = host_stream.map(move |batch_or_err| {
            let batch = match batch_or_err {
                Ok(batch) => batch,
                Err(err) => return Err(err),
            };
            let batch = cast_to_target_schema(batch, Arc::clone(&target_schema)).map_err(|err| {
                DataFusionError::ArrowError(Box::new(err), Some("Error while casting RecordBatch into a suitable schema for CuDF".to_string()))
            })?;

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

            RecordBatch::try_new(batch.schema(), cudf_cols).map_err(|err| {
                DataFusionError::ArrowError(Box::new(err), Some("Error while loading a RecordBatch into CuDF".to_string()))
            })
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, cudf_stream)))
    }
}

fn cudf_schema_compatibility_map(schema: SchemaRef) -> SchemaRef {
    let mut new_fields = Vec::with_capacity(schema.fields.len());

    for field in schema.fields() {
        let field = match field.data_type() {
            // CuDF doesn't support Utf8View, convert to regular Utf8
            DataType::Utf8View => FieldRef::new(Field::new(
                field.name(),
                DataType::Utf8,
                field.is_nullable(),
            )),
            // Normalize decimal precision to max for the representation type.
            // CuDF uses fixed precision based on storage type (int32/int64/int128),
            // so we normalize schema to match what CuDF will produce.
            // Scale is preserved as-is since it's user-specified metadata.
            DataType::Decimal32(_, s) => FieldRef::new(Field::new(
                field.name(),
                DataType::Decimal32(9, *s), // max precision for 32-bit
                field.is_nullable(),
            )),
            DataType::Decimal64(_, s) => FieldRef::new(Field::new(
                field.name(),
                DataType::Decimal64(18, *s), // max precision for 64-bit
                field.is_nullable(),
            )),
            DataType::Decimal128(_, s) => FieldRef::new(Field::new(
                field.name(),
                DataType::Decimal128(38, *s), // max precision for 128-bit
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
    let columns = batch
        .columns()
        .iter()
        .zip(target_schema.fields())
        .map(|(col, field)| arrow::compute::cast(col, field.data_type()))
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new(target_schema, columns)
}
