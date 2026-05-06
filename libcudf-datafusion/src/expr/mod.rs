use crate::expr::binary::CuDFBinaryExpr;
use crate::expr::column::CuDFColumnExpr;
use crate::expr::literal::CuDFLiteral;
use arrow::array::Array;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{exec_err, not_impl_err};
use datafusion::error::DataFusionError;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::expressions::{BinaryExpr, Column};
use datafusion_expr::ColumnarValue;
use datafusion_physical_plan::expressions::Literal;
use libcudf_rs::{CuDFColumnView, CuDFColumnViewOrScalar, CuDFScalar, CuDFStream};
use std::sync::Arc;

pub(crate) mod ast;
mod binary;
mod column;
mod literal;

pub(crate) fn columnar_value_to_cudf(
    c: ColumnarValue,
) -> Result<CuDFColumnViewOrScalar, DataFusionError> {
    match c {
        ColumnarValue::Array(arr) => {
            if let Some(cudf_col_view) = arr.as_any().downcast_ref::<CuDFColumnView>() {
                return Ok(cudf_col_view.clone().into());
            }
            if let Some(cudf_scalar) = arr.as_any().downcast_ref::<CuDFScalar>() {
                return Ok(cudf_scalar.clone().into());
            }
            exec_err!("ColumnarValue::Array is not CuDFColumnView or CuDFScalar")
        }
        ColumnarValue::Scalar(_) => {
            exec_err!("ColumnarValue::Scalar is not allowed when executing in CuDF nodes")
        }
    }
}

pub(crate) fn cudf_to_columnar_value(view: impl Into<CuDFColumnViewOrScalar>) -> ColumnarValue {
    match view.into() {
        CuDFColumnViewOrScalar::ColumnView(value) => ColumnarValue::Array(Arc::new(value)),
        CuDFColumnViewOrScalar::Scalar(value) => ColumnarValue::Array(Arc::new(value)),
    }
}

pub(crate) fn expr_to_cudf_expr(
    expr: &dyn PhysicalExpr,
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    let any = expr.as_any();
    if let Some(binary_op) = any.downcast_ref::<BinaryExpr>() {
        return Ok(Arc::new(CuDFBinaryExpr::from_host(binary_op.clone())?));
    };
    if let Some(column_expr) = any.downcast_ref::<Column>() {
        return Ok(Arc::new(CuDFColumnExpr::from_host(column_expr.clone())));
    };
    if let Some(literal) = any.downcast_ref::<Literal>() {
        return Ok(Arc::new(CuDFLiteral::from_host(literal.clone())?));
    }

    not_impl_err!("Expression {expr} not supported in CuDF")
}

pub(crate) fn expr_with_stream(
    expr: Arc<dyn PhysicalExpr>,
    stream: Option<Arc<CuDFStream>>,
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    let Some(stream) = stream else {
        return Ok(expr);
    };

    let transformed = expr.transform_up(|expr| {
        if let Some(binary) = expr.as_any().downcast_ref::<CuDFBinaryExpr>() {
            let streamed =
                Arc::new(binary.with_stream(Arc::clone(&stream))) as Arc<dyn PhysicalExpr>;
            Ok(Transformed::yes(streamed))
        } else {
            Ok(Transformed::no(expr))
        }
    })?;
    Ok(transformed.data)
}
