use crate::errors::cudf_to_df;
use crate::expr::{columnar_value_to_cudf, cudf_to_columnar_value};
use arrow::array::RecordBatch;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::BinaryExpr;
use datafusion::physical_expr::PhysicalExpr;
use libcudf_rs::cudf_binary_op;
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BinaryOp {
    expr: BinaryExpr,
}

impl Display for BinaryOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CuDF")?;
        self.expr.fmt(f)
    }
}

impl PhysicalExpr for BinaryOp {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        let output_type = self.expr.data_type(batch.schema_ref())?;

        let lhs = self.expr.left().evaluate(batch)?;
        let lhs = columnar_value_to_cudf(lhs)?;
        let rhs = self.expr.right().evaluate(batch)?;
        let rhs = columnar_value_to_cudf(rhs)?;

        let result = cudf_binary_op(lhs, rhs, 0, &output_type).map_err(cudf_to_df)?;

        Ok(cudf_to_columnar_value(result))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.expr.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        let expr = BinaryExpr::new(
            Arc::clone(&children[0]),
            *self.expr.op(),
            Arc::clone(&children[1]),
        );
        Ok(Arc::new(Self { expr }))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.expr.fmt_sql(f)
    }
}
