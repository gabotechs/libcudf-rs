use crate::errors::cudf_to_df;
use arrow::array::RecordBatch;
use datafusion::common::{exec_err, plan_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures_util::StreamExt;
use libcudf_rs::CuDFColumnView;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

#[derive(Debug)]
pub struct CuDFUnloadExec {
    input: Arc<dyn ExecutionPlan>,
}

impl CuDFUnloadExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        Self { input }
    }
}

impl DisplayAs for CuDFUnloadExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CudfLoadExec")
    }
}

impl ExecutionPlan for CuDFUnloadExec {
    fn name(&self) -> &str {
        "CuDFUnloadExec"
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
                "CuDFUnloadExec expects exactly 1 child, {} where provided",
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
        let cudf_stream = self.input.execute(partition, context)?;
        let schema = cudf_stream.schema();
        let host_stream = cudf_stream.map(|batch_or_err| {
            let batch = match batch_or_err {
                Ok(batch) => batch,
                Err(err) => return Err(err),
            };

            let original_cols = batch.columns();
            let mut host_cols = Vec::with_capacity(original_cols.len());
            for original_col in original_cols {
                let Some(cudf_col) = original_col.as_any().downcast_ref::<CuDFColumnView>() else {
                    return exec_err!(
                        "Cannot move RecordBatch from CuDF to host: a column is not a CuDF array"
                    );
                };
                let arr = cudf_col.to_arrow_host().map_err(cudf_to_df)?;
                host_cols.push(arr)
            }

            Ok(RecordBatch::try_new(batch.schema(), host_cols)?)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, host_stream)))
    }
}
