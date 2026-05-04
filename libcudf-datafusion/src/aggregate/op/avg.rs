use crate::aggregate::op::udf::CuDFAggregateUDF;
use crate::aggregate::CuDFAggregationOp;
use crate::errors::cudf_to_df;
use arrow_schema::DataType;
use datafusion::common::exec_err;
use datafusion::error::Result;
use datafusion::functions_aggregate::average::Avg;
use datafusion_expr::AggregateUDF;
use libcudf_rs::{cudf_binary_op, AggregationOp, AggregationRequest, CuDFBinaryOp, CuDFColumnView};
use std::fmt::Debug;
use std::sync::Arc;

pub fn avg() -> Arc<AggregateUDF> {
    let udf = CuDFAggregateUDF::new(Arc::new(Avg::default()), Arc::new(CuDFAvg));
    Arc::new(AggregateUDF::new_from_impl(udf))
}

#[derive(Debug, Default)]
pub struct CuDFAvg;

impl CuDFAggregationOp for CuDFAvg {
    fn partial_requests(&self, args: &[CuDFColumnView]) -> Result<Vec<AggregationRequest>> {
        // Partial: COUNT and SUM input values per batch
        if args.len() != 1 {
            return exec_err!("AVG expects 1 argument, got {}", args.len());
        }

        let mut sum_request = AggregationRequest::from_column_view(args[0].clone());
        sum_request.add(AggregationOp::SUM.group_by());

        let mut count_request = AggregationRequest::from_column_view(args[0].clone());
        count_request.add(AggregationOp::COUNT.group_by());

        Ok(vec![sum_request, count_request])
    }

    fn final_requests(&self, args: &[CuDFColumnView]) -> Result<Vec<AggregationRequest>> {
        // Final: SUM the partial sums and counts
        if args.len() != 2 {
            return exec_err!("AVG final expects 2 arguments, got {}", args.len());
        }

        let mut sum_request = AggregationRequest::from_column_view(args[0].clone());
        sum_request.add(AggregationOp::SUM.group_by());

        let mut count_request = AggregationRequest::from_column_view(args[1].clone());
        count_request.add(AggregationOp::SUM.group_by());

        Ok(vec![sum_request, count_request])
    }

    fn merge(&self, args: &[CuDFColumnView]) -> Result<CuDFColumnView> {
        // Merge: final sum / count
        if args.len() != 2 {
            return exec_err!("AVG merge expects 2 argument, got {}", args.len());
        }

        let result = cudf_binary_op(
            libcudf_rs::CuDFColumnViewOrScalar::ColumnView(args[0].clone()),
            libcudf_rs::CuDFColumnViewOrScalar::ColumnView(args[1].clone()),
            CuDFBinaryOp::Div,
            &DataType::Float64,
        )
        .map_err(cudf_to_df)?;

        match result {
            libcudf_rs::CuDFColumnViewOrScalar::ColumnView(view) => Ok(view),
            libcudf_rs::CuDFColumnViewOrScalar::Scalar(_) => {
                exec_err!("AVG merge expects ColumnView, got Scalar")
            }
        }
    }
}
