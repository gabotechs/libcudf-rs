use crate::{arrow_type_to_cudf, CuDFColumnView, CuDFError, CudfColumnViewOrScalar};
use arrow_schema::{ArrowError, DataType};

pub fn cudf_binary_op(
    left: CudfColumnViewOrScalar,
    right: CudfColumnViewOrScalar,
    op: i32,
    output_type: &DataType,
) -> Result<CudfColumnViewOrScalar, CuDFError> {
    let Some(dt) = arrow_type_to_cudf(output_type) else {
        return Err(ArrowError::NotYetImplemented(format!(
            "Output type {output_type} not supported in CuDF"
        )))?;
    };

    let result = match (left, right) {
        (CudfColumnViewOrScalar::ColumnView(lhs), CudfColumnViewOrScalar::ColumnView(rhs)) => {
            libcudf_sys::ffi::binary_operation_col_col(lhs.inner(), rhs.inner(), op, dt)
        }
        (CudfColumnViewOrScalar::ColumnView(lhs), CudfColumnViewOrScalar::Scalar(rhs)) => {
            libcudf_sys::ffi::binary_operation_col_scalar(lhs.inner(), rhs.inner(), op, dt)
        }
        (CudfColumnViewOrScalar::Scalar(lhs), CudfColumnViewOrScalar::ColumnView(rhs)) => {
            libcudf_sys::ffi::binary_operation_scalar_col(lhs.inner(), rhs.inner(), op, dt)
        }
        (CudfColumnViewOrScalar::Scalar(_), CudfColumnViewOrScalar::Scalar(_)) => {
            return Err(ArrowError::InvalidArgumentError("".to_string()))?
        }
    }?;
    Ok(CudfColumnViewOrScalar::ColumnView(
        CuDFColumnView::from_column(result),
    ))
}
