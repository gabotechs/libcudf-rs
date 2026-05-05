use crate::errors::cudf_to_df;
use arrow::array::Scalar;
use arrow_schema::{
    DataType, DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DECIMAL32_MAX_PRECISION,
    DECIMAL32_MAX_SCALE, DECIMAL64_MAX_PRECISION, DECIMAL64_MAX_SCALE,
};
use datafusion::common::{exec_err, DataFusionError};
use libcudf_rs::{cudf_binary_op, CuDFBinaryOp, CuDFColumnViewOrScalar, CuDFScalar};

pub(crate) fn is_decimal_division(
    output_type: &DataType,
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> bool {
    is_supported_decimal(output_type)
        && is_supported_decimal(lhs_type)
        && is_supported_decimal(rhs_type)
}

pub(crate) fn is_supported_decimal(data_type: &DataType) -> bool {
    decimal_parts(data_type).is_some()
}

pub(crate) fn decimal_count_type_for(data_type: &DataType) -> Result<DataType, DataFusionError> {
    decimal_type_with_scale(data_type, 0)
}

pub(crate) fn decimal_div(
    lhs: CuDFColumnViewOrScalar,
    rhs: CuDFColumnViewOrScalar,
    lhs_type: &DataType,
    rhs_type: &DataType,
    output_type: &DataType,
) -> Result<CuDFColumnViewOrScalar, DataFusionError> {
    let (_, lhs_scale) = decimal_parts_or_err(lhs_type, "left operand")?;
    let (_, rhs_scale) = decimal_parts_or_err(rhs_type, "right operand")?;
    let (_, output_scale) = decimal_parts_or_err(output_type, "output")?;

    // DataFusion scales the stored numerator before decimal division. cuDF's
    // fixed-point Div divides the stored integers directly, so rescale first.
    // Example: 3.00 / 100.00 with output scale 6 is stored as
    // (300 * 10^6) / 10000 = 30000, which represents 0.030000.
    let scale_delta = i16::from(output_scale) - i16::from(lhs_scale) + i16::from(rhs_scale);

    let (lhs, rhs) = if scale_delta >= 0 {
        let target_scale = checked_scale(i16::from(lhs_scale) + scale_delta)?;
        (rescale_decimal(lhs, lhs_type, target_scale)?, rhs)
    } else {
        let target_scale = checked_scale(i16::from(rhs_scale) - scale_delta)?;
        (lhs, rescale_decimal(rhs, rhs_type, target_scale)?)
    };

    cudf_binary_op(lhs, rhs, CuDFBinaryOp::Div, output_type).map_err(cudf_to_df)
}

fn rescale_decimal(
    value: CuDFColumnViewOrScalar,
    data_type: &DataType,
    target_scale: i8,
) -> Result<CuDFColumnViewOrScalar, DataFusionError> {
    let (_, current_scale) = decimal_parts_or_err(data_type, "operand")?;
    if target_scale == current_scale {
        return Ok(value);
    }

    let target_type = decimal_type_with_scale(data_type, target_scale)?;
    match value {
        CuDFColumnViewOrScalar::ColumnView(view) => Ok(libcudf_rs::cast(&view, &target_type)
            .map_err(cudf_to_df)?
            .into_view()
            .into()),
        CuDFColumnViewOrScalar::Scalar(scalar) => {
            let array = scalar.to_arrow_host().map_err(cudf_to_df)?;
            let casted = arrow::compute::cast(array.as_ref(), &target_type)
                .map_err(|err| DataFusionError::ArrowError(Box::new(err), None))?;
            Ok(CuDFScalar::from_arrow_host(Scalar::new(casted))
                .map_err(cudf_to_df)?
                .into())
        }
    }
}

pub(crate) fn decimal_parts(data_type: &DataType) -> Option<(u8, i8)> {
    match data_type {
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale) => Some((*precision, *scale)),
        _ => None,
    }
}

fn decimal_parts_or_err(data_type: &DataType, role: &str) -> Result<(u8, i8), DataFusionError> {
    decimal_parts(data_type).ok_or_else(|| {
        DataFusionError::Execution(format!("Expected decimal {role}, got {data_type}"))
    })
}

fn decimal_type_with_scale(data_type: &DataType, scale: i8) -> Result<DataType, DataFusionError> {
    let data_type = match data_type {
        DataType::Decimal32(_, _)
            if (-DECIMAL32_MAX_SCALE..=DECIMAL32_MAX_SCALE).contains(&scale) =>
        {
            DataType::Decimal32(DECIMAL32_MAX_PRECISION, scale)
        }
        DataType::Decimal64(_, _)
            if (-DECIMAL64_MAX_SCALE..=DECIMAL64_MAX_SCALE).contains(&scale) =>
        {
            DataType::Decimal64(DECIMAL64_MAX_PRECISION, scale)
        }
        DataType::Decimal128(_, _)
            if (-DECIMAL128_MAX_SCALE..=DECIMAL128_MAX_SCALE).contains(&scale) =>
        {
            DataType::Decimal128(DECIMAL128_MAX_PRECISION, scale)
        }
        _ => {
            return exec_err!("Cannot rescale decimal operand of type {data_type} to scale {scale}")
        }
    };
    Ok(data_type)
}

fn checked_scale(scale: i16) -> Result<i8, DataFusionError> {
    i8::try_from(scale)
        .map_err(|_| DataFusionError::Execution(format!("Decimal scale {scale} is out of range")))
}
