use crate::aggregate::op::udf::CuDFAggregateUDF;
use crate::aggregate::CuDFAggregationOp;
use crate::errors::cudf_to_df;
use arrow::compute::cast;
use arrow_schema::DataType;
use datafusion::common::exec_err;
use datafusion::error::Result;
use datafusion::functions_aggregate::count::Count;
use datafusion_expr::AggregateUDF;
use libcudf_rs::{AggregationOp, AggregationRequest, CuDFColumnView};
use std::fmt::Debug;
use std::sync::Arc;

pub fn count() -> Arc<AggregateUDF> {
    let udf = CuDFAggregateUDF::new(Arc::new(Count::default()), Arc::new(CuDFCount));
    Arc::new(AggregateUDF::new_from_impl(udf))
}

#[derive(Debug, Default)]
pub struct CuDFCount;

impl CuDFAggregationOp for CuDFCount {
    fn partial_requests(&self, args: &[CuDFColumnView]) -> Result<Vec<AggregationRequest>> {
        // Partial: COUNT input values per batch
        if args.len() != 1 {
            return exec_err!("COUNT expects 1 argument, got {}", args.len());
        }

        let mut request = AggregationRequest::from_column_view(args[0].clone());
        request.add(AggregationOp::COUNT.group_by());

        Ok(vec![request])
    }

    fn final_requests(&self, args: &[CuDFColumnView]) -> Result<Vec<AggregationRequest>> {
        // Final: SUM the partial counts
        if args.len() != 1 {
            return exec_err!("COUNT final expects 1 argument, got {}", args.len());
        }

        let mut request = AggregationRequest::from_column_view(args[0].clone());
        request.add(AggregationOp::SUM.group_by());

        Ok(vec![request])
    }

    fn merge(&self, args: &[CuDFColumnView]) -> Result<CuDFColumnView> {
        if args.len() != 1 {
            return exec_err!("COUNT merge expects 1 argument, got {}", args.len());
        }

        // cuDF COUNT returns Int32, but DataFusion expects Int64
        // Cast to Int64 to match DataFusion's expectation
        let result_array = args[0].to_arrow_host().map_err(cudf_to_df)?;
        let casted = cast(&result_array, &DataType::Int64)?;

        // Convert back to CuDFColumn and return view
        let column =
            libcudf_rs::CuDFColumn::from_arrow_host(casted.as_ref()).map_err(cudf_to_df)?;
        Ok(column.into_view())
    }
}
