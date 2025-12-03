use arrow::array::Array;
use datafusion::common::exec_err;
use datafusion::error::DataFusionError;
use datafusion_expr::ColumnarValue;
use libcudf_rs::{CuDFColumnView, CuDFScalar, CudfColumnViewOrScalar};
use std::sync::Arc;

mod binary_op;

pub(crate) fn columnar_value_to_cudf(
    c: ColumnarValue,
) -> Result<CudfColumnViewOrScalar, DataFusionError> {
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
        ColumnarValue::Scalar(s) => {
            exec_err!("ColumnarValue::Scalar is not allowed when executing in CuDF nodes")
        }
    }
}

pub(crate) fn cudf_to_columnar_value(view: impl Into<CudfColumnViewOrScalar>) -> ColumnarValue {
    match view.into() {
        CudfColumnViewOrScalar::ColumnView(value) => ColumnarValue::Array(Arc::new(value)),
        CudfColumnViewOrScalar::Scalar(value) => ColumnarValue::Array(Arc::new(value)),
    }
}
