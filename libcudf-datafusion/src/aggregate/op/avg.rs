use crate::aggregate::op::udf::CuDFAggregateUDF;
use crate::aggregate::CuDFAggregationOp;
use crate::errors::cudf_to_df;
use arrow_schema::DataType;
use datafusion::common::exec_err;
use datafusion::error::Result;
use datafusion::functions_aggregate::average::Avg;
use datafusion_expr::AggregateUDF;
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

    fn partial_requests(&self, args: &[CuDFColumnView]) -> Result<Vec<AggregationRequest>> {
        if args.len() != 1 {
            return exec_err!("AVG expects 1 argument, got {}", args.len());
        }

        let mut count_request = AggregationRequest::from_column_view(args[0].clone());
        count_request.add(AggregationOp::COUNT.group_by());

        let mut sum_request = AggregationRequest::from_column_view(args[0].clone());
        sum_request.add(AggregationOp::SUM.group_by());

        Ok(vec![count_request, sum_request])
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

    fn finalize(&self, state_cols: &[CuDFColumnView]) -> Result<CuDFColumnView> {
        if state_cols.len() != 2 {
            return exec_err!(
                "AVG finalize expects 2 state columns, got {}",
                state_cols.len()
            );
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
