use crate::column::CuDFColumn;
use crate::{arrow_type_to_cudf, CuDFColumnViewOrScalar, CuDFError};
use arrow_schema::{ArrowError, DataType};

/// Binary operations supported by cuDF
///
/// Maps to cuDF's `binary_operator` enum
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CuDFBinaryOp {
    /// operator +
    Add = 0,
    /// operator -
    Sub = 1,
    /// operator *
    Mul = 2,
    /// operator / using common type of lhs and rhs
    Div = 3,
    /// operator / after promoting type to floating point
    TrueDiv = 4,
    /// operator // (floor division)
    FloorDiv = 5,
    /// operator %
    Mod = 6,
    /// positive modulo operator
    PMod = 7,
    /// operator % but following Python's sign rules for negatives
    PyMod = 8,
    /// lhs ^ rhs
    Pow = 9,
    /// int ^ int, used to avoid floating point precision loss
    IntPow = 10,
    /// logarithm to the base
    LogBase = 11,
    /// 2-argument arctangent
    Atan2 = 12,
    /// operator <<
    ShiftLeft = 13,
    /// operator >>
    ShiftRight = 14,
    /// operator >>> (logical right shift, from Java)
    ShiftRightUnsigned = 15,
    /// operator &
    BitwiseAnd = 16,
    /// operator |
    BitwiseOr = 17,
    /// operator ^
    BitwiseXor = 18,
    /// operator &&
    LogicalAnd = 19,
    /// operator ||
    LogicalOr = 20,
    /// operator ==
    Equal = 21,
    /// operator !=
    NotEqual = 22,
    /// operator <
    Less = 23,
    /// operator >
    Greater = 24,
    /// operator <=
    LessEqual = 25,
    /// operator >=
    GreaterEqual = 26,
    /// Returns true when both operands are null; false when one is null;
    /// the result of equality when both are non-null
    NullEquals = 27,
    /// Returns false when both operands are null; true when one is null;
    /// the result of inequality when both are non-null
    NullNotEquals = 28,
    /// Returns max of operands when both are non-null; returns the non-null
    /// operand when one is null; or invalid when both are null
    NullMax = 29,
    /// Returns min of operands when both are non-null; returns the non-null
    /// operand when one is null; or invalid when both are null
    NullMin = 30,
    /// generic binary operator to be generated with input ptx code
    GenericBinary = 31,
    /// operator && with Spark rules
    NullLogicalAnd = 32,
    /// operator || with Spark rules
    NullLogicalOr = 33,
}

pub fn cudf_binary_op(
    left: CuDFColumnViewOrScalar,
    right: CuDFColumnViewOrScalar,
    op: CuDFBinaryOp,
    output_type: &DataType,
) -> Result<CuDFColumnViewOrScalar, CuDFError> {
    let Some(dt) = arrow_type_to_cudf(output_type) else {
        return Err(ArrowError::NotYetImplemented(format!(
            "Output type {output_type} not supported in CuDF"
        )))?;
    };

    let result = match (left, right) {
        (CuDFColumnViewOrScalar::ColumnView(lhs), CuDFColumnViewOrScalar::ColumnView(rhs)) => {
            libcudf_sys::ffi::binary_operation_col_col(lhs.inner(), rhs.inner(), op as i32, dt)
        }
        (CuDFColumnViewOrScalar::ColumnView(lhs), CuDFColumnViewOrScalar::Scalar(rhs)) => {
            libcudf_sys::ffi::binary_operation_col_scalar(lhs.inner(), rhs.inner(), op as i32, dt)
        }
        (CuDFColumnViewOrScalar::Scalar(lhs), CuDFColumnViewOrScalar::ColumnView(rhs)) => {
            libcudf_sys::ffi::binary_operation_scalar_col(lhs.inner(), rhs.inner(), op as i32, dt)
        }
        (CuDFColumnViewOrScalar::Scalar(_), CuDFColumnViewOrScalar::Scalar(_)) => {
            return Err(ArrowError::InvalidArgumentError("".to_string()))?
        }
    }?;
    Ok(CuDFColumnViewOrScalar::ColumnView(
        CuDFColumn::new(result).into_view(),
    ))
}
