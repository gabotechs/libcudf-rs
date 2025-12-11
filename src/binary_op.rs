use crate::column::CuDFColumn;
use crate::data_type::arrow_type_to_cudf_data_type;
use crate::{CuDFColumnViewOrScalar, CuDFError};
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
    let Some(dt) = arrow_type_to_cudf_data_type(output_type) else {
        return Err(ArrowError::NotYetImplemented(format!(
            "Output type {output_type} not supported in CuDF"
        )))?;
    };

    let result = match (left, right) {
        (CuDFColumnViewOrScalar::ColumnView(lhs), CuDFColumnViewOrScalar::ColumnView(rhs)) => {
            libcudf_sys::ffi::binary_operation_col_col(lhs.inner(), rhs.inner(), op as i32, &dt)
        }
        (CuDFColumnViewOrScalar::ColumnView(lhs), CuDFColumnViewOrScalar::Scalar(rhs)) => {
            libcudf_sys::ffi::binary_operation_col_scalar(lhs.inner(), rhs.inner(), op as i32, &dt)
        }
        (CuDFColumnViewOrScalar::Scalar(lhs), CuDFColumnViewOrScalar::ColumnView(rhs)) => {
            libcudf_sys::ffi::binary_operation_scalar_col(lhs.inner(), rhs.inner(), op as i32, &dt)
        }
        (CuDFColumnViewOrScalar::Scalar(_), CuDFColumnViewOrScalar::Scalar(_)) => {
            return Err(ArrowError::InvalidArgumentError("".to_string()))?
        }
    }?;
    Ok(CuDFColumnViewOrScalar::ColumnView(
        CuDFColumn::new(result).into_view(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Decimal128Array};
    use arrow::datatypes::DataType;

    #[test]
    fn test_decimal_addition() -> Result<(), Box<dyn std::error::Error>> {
        // Create two decimal columns with scale=2 (e.g., currency values)
        let values1 = vec![12345i128, 67890i128, 11111i128]; // 123.45, 678.90, 111.11
        let values2 = vec![54321i128, 98765i128, 22222i128]; // 543.21, 987.65, 222.22

        let array1 = Decimal128Array::from(values1).with_precision_and_scale(38, 2)?;
        let array2 = Decimal128Array::from(values2).with_precision_and_scale(38, 2)?;

        let col1 = CuDFColumn::from_arrow_host(&array1)?;
        let col2 = CuDFColumn::from_arrow_host(&array2)?;

        // Add the two decimal columns
        let result = cudf_binary_op(
            CuDFColumnViewOrScalar::ColumnView(col1.into_view()),
            CuDFColumnViewOrScalar::ColumnView(col2.into_view()),
            CuDFBinaryOp::Add,
            &DataType::Decimal128(38, 2),
        )?;

        // Convert result back to Arrow
        let result_col = match result {
            CuDFColumnViewOrScalar::ColumnView(view) => view,
            _ => panic!("Expected column view"),
        };

        let result_array = result_col.to_arrow_host()?;
        let result_decimal = result_array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("Expected Decimal128Array");

        // Verify results: 123.45 + 543.21 = 666.66, etc.
        assert_eq!(result_decimal.len(), 3);
        assert_eq!(result_decimal.value(0), 66666); // 666.66 with scale=2
        assert_eq!(result_decimal.value(1), 166655); // 1666.55 with scale=2
        assert_eq!(result_decimal.value(2), 33333); // 333.33 with scale=2

        Ok(())
    }

    #[test]
    fn test_decimal_multiplication() -> Result<(), Box<dyn std::error::Error>> {
        // Create decimal columns: price * quantity
        let prices = vec![1050i128, 2500i128]; // 10.50, 25.00 with scale=2
        let quantities = vec![3i128, 5i128]; // 3, 5 with scale=0

        let price_array = Decimal128Array::from(prices).with_precision_and_scale(38, 2)?;
        let qty_array = Decimal128Array::from(quantities).with_precision_and_scale(38, 0)?;

        let col_price = CuDFColumn::from_arrow_host(&price_array)?;
        let col_qty = CuDFColumn::from_arrow_host(&qty_array)?;

        // Multiply: result should have scale = 2 + 0 = 2
        let result = cudf_binary_op(
            CuDFColumnViewOrScalar::ColumnView(col_price.into_view()),
            CuDFColumnViewOrScalar::ColumnView(col_qty.into_view()),
            CuDFBinaryOp::Mul,
            &DataType::Decimal128(38, 2),
        )?;

        let result_col = match result {
            CuDFColumnViewOrScalar::ColumnView(view) => view,
            _ => panic!("Expected column view"),
        };

        let result_array = result_col.to_arrow_host()?;
        let result_decimal = result_array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("Expected Decimal128Array");

        // Verify: 10.50 * 3 = 31.50, 25.00 * 5 = 125.00
        assert_eq!(result_decimal.value(0), 3150); // 31.50 with scale=2
        assert_eq!(result_decimal.value(1), 12500); // 125.00 with scale=2

        Ok(())
    }
}
