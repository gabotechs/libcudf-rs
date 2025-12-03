use crate::{arrow_type_to_cudf, CuDFColumnView, CuDFColumnViewOrScalar, CuDFError};
use arrow_schema::{ArrowError, DataType};

pub fn cudf_binary_op(
    left: CuDFColumnViewOrScalar,
    right: CuDFColumnViewOrScalar,
    op: i32,
    output_type: &DataType,
) -> Result<CuDFColumnViewOrScalar, CuDFError> {
    let Some(dt) = arrow_type_to_cudf(output_type) else {
        return Err(ArrowError::NotYetImplemented(format!(
            "Output type {output_type} not supported in CuDF"
        )))?;
    };

    let result = match (left, right) {
        (CuDFColumnViewOrScalar::ColumnView(lhs), CuDFColumnViewOrScalar::ColumnView(rhs)) => {
            libcudf_sys::ffi::binary_operation_col_col(lhs.inner(), rhs.inner(), op, dt)
        }
        (CuDFColumnViewOrScalar::ColumnView(lhs), CuDFColumnViewOrScalar::Scalar(rhs)) => {
            libcudf_sys::ffi::binary_operation_col_scalar(lhs.inner(), rhs.inner(), op, dt)
        }
        (CuDFColumnViewOrScalar::Scalar(lhs), CuDFColumnViewOrScalar::ColumnView(rhs)) => {
            libcudf_sys::ffi::binary_operation_scalar_col(lhs.inner(), rhs.inner(), op, dt)
        }
        (CuDFColumnViewOrScalar::Scalar(_), CuDFColumnViewOrScalar::Scalar(_)) => {
            return Err(ArrowError::InvalidArgumentError("".to_string()))?
        }
    }?;
    Ok(CuDFColumnViewOrScalar::ColumnView(
        CuDFColumnView::from_column(result),
    ))
}
