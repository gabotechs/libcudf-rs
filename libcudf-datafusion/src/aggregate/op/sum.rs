use crate::aggregate::CuDFAggregationOp;
use crate::errors::cudf_to_df;
use datafusion::common::exec_err;
use datafusion::error::Result;
use libcudf_rs::{AggregationOp, AggregationRequest, CuDFColumnView};
use std::fmt::Debug;
use std::sync::Arc;
use datafusion::functions_aggregate::sum::{sum_udaf, Sum};
use datafusion_expr::AggregateUDF;
use crate::aggregate::op::udf::CuDFAggregateUDF;

pub fn sum() -> Arc<AggregateUDF> {
    let udf = CuDFAggregateUDF::new(Arc::new(Sum::default()), Arc::new(CuDFSum));
    Arc::new(AggregateUDF::new_from_impl(udf))
}


#[derive(Debug, Default)]
pub struct CuDFSum;

impl CuDFAggregationOp for CuDFSum {
    fn partial_requests(&self, args: &[CuDFColumnView]) -> Result<Vec<AggregationRequest>> {
        self.final_requests(args)
    }

    fn final_requests(&self, args: &[CuDFColumnView]) -> Result<Vec<AggregationRequest>> {
        if args.len() != 1 {
            return exec_err!("SUM expects 1 argument, got {}", args.len());
        }

        let view = CuDFColumnView::from_arrow(&args[0]).map_err(cudf_to_df)?;
        let mut request = AggregationRequest::new(&view);
        request.add(AggregationOp::SUM.group_by());

        Ok(vec![request])
    }

    fn merge(&self, args: &[CuDFColumnView]) -> Result<CuDFColumnView> {
        if args.len() != 1 {
            return exec_err!("SUM merge expects 1 argument, got {}", args.len());
        }

        Ok(args[0].clone())
    }
}
