use crate::decimal::{decimal_count_type_for, decimal_div, is_supported_decimal};
use crate::errors::cudf_to_df;
use crate::physical::aggregate::op::udf::CuDFAggregateUDF;
use crate::physical::aggregate::CuDFAggregationOp;
use arrow::array::Array;
use arrow_schema::DataType;
use datafusion::common::exec_err;
use datafusion::error::Result;
use datafusion::functions_aggregate::average::Avg;
use datafusion_expr::AggregateUDF;
use datafusion_physical_plan::aggregates::AggregateMode;
use libcudf_rs::{
    cudf_binary_op, AggregationOp, AggregationRequest, CuDFBinaryOp, CuDFColumn, CuDFColumnView,
    CuDFColumnViewOrScalar,
};
use std::sync::Arc;

pub fn avg() -> Arc<AggregateUDF> {
    let udf = CuDFAggregateUDF::new(Arc::new(Avg::default()), Arc::new(CuDFAvg));
    Arc::new(AggregateUDF::new_from_impl(udf))
}

#[derive(Debug, Default)]
pub struct CuDFAvg;

impl CuDFAggregationOp for CuDFAvg {
    fn num_state_columns(&self) -> usize {
        2 // [count, sum]
    }

    fn supports_input_types(
        &self,
        mode: AggregateMode,
        input_types: &[DataType],
        output_type: &DataType,
    ) -> bool {
        match mode {
            AggregateMode::Final | AggregateMode::FinalPartitioned => {
                supports_final_input_types(input_types, output_type)
            }
            _ => supports_raw_input_types(input_types, output_type),
        }
    }

    fn partial_requests(&self, args: &[CuDFColumnView]) -> Result<Vec<AggregationRequest>> {
        if args.len() != 1 {
            return exec_err!("AVG expects 1 argument, got {}", args.len());
        }

        let mut request = AggregationRequest::from_column_view(args[0].clone());
        request.add(AggregationOp::COUNT.group_by());
        request.add(AggregationOp::SUM.group_by());

        Ok(vec![request])
    }

    fn normalize_partial_state(&self, mut cols: Vec<CuDFColumn>) -> Result<Vec<CuDFColumn>> {
        // count column: cuDF COUNT returns Int32 -> cast to Int64 to match merge_requests output
        let sum = cols.remove(1);
        let count = cols.remove(0);
        let casted_count =
            libcudf_rs::cast(&count.into_view(), &DataType::Int64).map_err(cudf_to_df)?;
        Ok(vec![casted_count, sum])
    }

    fn merge_requests(&self, state_cols: &[CuDFColumnView]) -> Result<Vec<AggregationRequest>> {
        if state_cols.len() != 2 {
            return exec_err!(
                "AVG merge expects 2 state columns, got {}",
                state_cols.len()
            );
        }

        let mut count_request = AggregationRequest::from_column_view(state_cols[0].clone());
        count_request.add(AggregationOp::SUM.group_by());

        let mut sum_request = AggregationRequest::from_column_view(state_cols[1].clone());
        sum_request.add(AggregationOp::SUM.group_by());

        Ok(vec![count_request, sum_request])
    }

    fn finalize(
        &self,
        state_cols: &[CuDFColumnView],
        output_type: &DataType,
    ) -> Result<CuDFColumnView> {
        if state_cols.len() != 2 {
            return exec_err!(
                "AVG finalize expects 2 state columns, got {}",
                state_cols.len()
            );
        }

        if is_supported_decimal(output_type) {
            return finalize_decimal_avg(&state_cols[1], &state_cols[0], output_type);
        }

        let result = cudf_binary_op(
            CuDFColumnViewOrScalar::ColumnView(state_cols[1].clone()),
            CuDFColumnViewOrScalar::ColumnView(state_cols[0].clone()),
            CuDFBinaryOp::Div,
            &DataType::Float64,
        )
        .map_err(cudf_to_df)?;

        match result {
            CuDFColumnViewOrScalar::ColumnView(view) => Ok(view),
            CuDFColumnViewOrScalar::Scalar(_) => {
                exec_err!("AVG finalize: expected ColumnView from division, got Scalar")
            }
        }
    }
}

fn finalize_decimal_avg(
    sum: &CuDFColumnView,
    count: &CuDFColumnView,
    output_type: &DataType,
) -> Result<CuDFColumnView> {
    let sum_type = sum.data_type().clone();
    let count_type = decimal_count_type_for(&sum_type)?;
    let count = libcudf_rs::cast(count, &count_type)
        .map_err(cudf_to_df)?
        .into_view();

    let result = decimal_div(
        CuDFColumnViewOrScalar::ColumnView(sum.clone()),
        CuDFColumnViewOrScalar::ColumnView(count),
        &sum_type,
        &count_type,
        output_type,
    )?;

    match result {
        CuDFColumnViewOrScalar::ColumnView(view) => Ok(view),
        CuDFColumnViewOrScalar::Scalar(_) => {
            exec_err!("AVG decimal finalize: expected ColumnView from division, got Scalar")
        }
    }
}

fn supports_raw_input_types(input_types: &[DataType], output_type: &DataType) -> bool {
    let [input_type] = input_types else {
        return false;
    };

    supports_avg_type_pair(input_type, output_type)
}

fn supports_final_input_types(input_types: &[DataType], output_type: &DataType) -> bool {
    let [count_type, sum_type] = input_types else {
        return false;
    };

    count_type.is_integer() && supports_avg_type_pair(sum_type, output_type)
}

fn supports_avg_type_pair(input_type: &DataType, output_type: &DataType) -> bool {
    match (
        is_supported_decimal(input_type),
        is_supported_decimal(output_type),
    ) {
        (true, true) => true,
        (true, false) | (false, true) => false,
        (false, false) => {
            !matches!(input_type, DataType::Decimal256(_, _))
                && !matches!(output_type, DataType::Decimal256(_, _))
        }
    }
}
