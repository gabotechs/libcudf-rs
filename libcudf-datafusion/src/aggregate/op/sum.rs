use crate::aggregate::op::udf::CuDFAggregateUDF;
use crate::aggregate::CuDFAggregationOp;
use datafusion::common::exec_err;
use datafusion::error::Result;
use datafusion::functions_aggregate::sum::Sum;
use datafusion_expr::AggregateUDF;
use libcudf_rs::{AggregationOp, AggregationRequest, CuDFColumnView};
use std::fmt::Debug;
use std::sync::Arc;

pub fn sum() -> Arc<AggregateUDF> {
    let udf = CuDFAggregateUDF::new(Arc::new(Sum::default()), Arc::new(CuDFSum));
    Arc::new(AggregateUDF::new_from_impl(udf))
}

#[derive(Debug, Default)]
pub struct CuDFSum;

impl CuDFAggregationOp for CuDFSum {
    fn num_state_columns(&self) -> usize {
        1
    }

    fn partial_requests(&self, args: &[CuDFColumnView]) -> Result<Vec<AggregationRequest>> {
        if args.len() != 1 {
            return exec_err!("SUM expects 1 argument, received: {}", args.len());
        }

        let mut request = AggregationRequest::from_column_view(args[0].clone());
        request.add(AggregationOp::SUM.group_by());
        Ok(vec![request])
    }

    fn merge_requests(&self, state_cols: &[CuDFColumnView]) -> Result<Vec<AggregationRequest>> {
        if state_cols.len() != 1 {
            return exec_err!(
                "SUM merge expects 1 state column, received: {}",
                state_cols.len()
            );
        }

        let mut request = AggregationRequest::from_column_view(state_cols[0].clone());
        request.add(AggregationOp::SUM.group_by());
        Ok(vec![request])
    }

    fn finalize(&self, state_cols: &[CuDFColumnView]) -> Result<CuDFColumnView> {
        if state_cols.len() != 1 {
            return exec_err!(
                "SUM finalize expects 1 state column, received: {}",
                state_cols.len()
            );
        }
        Ok(state_cols[0].clone())
    }
}
