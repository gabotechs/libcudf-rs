use crate::aggregate::op::udf::CuDFAggregateUDF;
use crate::aggregate::CuDFAggregationOp;
use crate::errors::cudf_to_df;
use arrow_schema::DataType;
use datafusion::common::exec_err;
use datafusion::error::Result;
use datafusion::functions_aggregate::count::Count;
use datafusion_expr::AggregateUDF;
use libcudf_rs::{AggregationOp, AggregationRequest, CuDFColumn, CuDFColumnView};
use std::sync::Arc;

pub fn count() -> Arc<AggregateUDF> {
    let udf = CuDFAggregateUDF::new(Arc::new(Count::default()), Arc::new(CuDFCount));
    Arc::new(AggregateUDF::new_from_impl(udf))
}

#[derive(Debug, Default)]
pub struct CuDFCount;

impl CuDFAggregationOp for CuDFCount {
    fn num_state_columns(&self) -> usize {
        1
    }

    fn partial_requests(&self, args: &[CuDFColumnView]) -> Result<Vec<AggregationRequest>> {
        if args.len() != 1 {
            return exec_err!("COUNT expects 1 argument, got {}", args.len());
        }

        let mut request = AggregationRequest::from_column_view(args[0].clone());
        request.add(AggregationOp::COUNT.group_by());
        Ok(vec![request])
    }

    fn merge_requests(&self, state_cols: &[CuDFColumnView]) -> Result<Vec<AggregationRequest>> {
        if state_cols.len() != 1 {
            return exec_err!(
                "COUNT merge expects 1 state column, got {}",
                state_cols.len()
            );
        }

        let mut request = AggregationRequest::from_column_view(state_cols[0].clone());
        request.add(AggregationOp::SUM.group_by());
        Ok(vec![request])
    }

    fn normalize_partial_state(&self, mut cols: Vec<CuDFColumn>) -> Result<Vec<CuDFColumn>> {
        // cuDF COUNT returns Int32 -> cast to Int64 to match merge_requests (SUM) output type
        let casted =
            libcudf_rs::cast(&cols.remove(0).into_view(), &DataType::Int64).map_err(cudf_to_df)?;
        Ok(vec![casted])
    }

    fn finalize(&self, state_cols: &[CuDFColumnView]) -> Result<CuDFColumnView> {
        if state_cols.len() != 1 {
            return exec_err!(
                "COUNT finalize expects 1 state column, got {}",
                state_cols.len()
            );
        }
        // cuDF COUNT returns Int32, DataFusion expects Int64
        let casted = libcudf_rs::cast(&state_cols[0], &DataType::Int64).map_err(cudf_to_df)?;
        Ok(casted.into_view())
    }
}
