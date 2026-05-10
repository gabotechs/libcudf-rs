use crate::errors::cudf_to_df;
use crate::physical::aggregate::op::udf::CuDFAggregateUDF;
use crate::physical::aggregate::{CuDFAggregationOp, ExprKey, ReusableStateKey};
use arrow_schema::{DataType, SchemaRef};
use datafusion::common::exec_err;
use datafusion::error::Result;
use datafusion::functions_aggregate::count::Count;
use datafusion_expr::AggregateUDF;
use datafusion_physical_plan::PhysicalExpr;
use libcudf_rs::{AggregationOp, AggregationRequest, CuDFColumn, CuDFColumnView};
use std::sync::Arc;

pub fn count() -> Arc<AggregateUDF> {
    let udf = CuDFAggregateUDF::new(Arc::new(Count::default()), Arc::new(CuDFCount::default()));
    Arc::new(AggregateUDF::new_from_impl(udf))
}

#[derive(Debug, Default)]
pub struct CuDFCount {
    count_nulls: bool,
}

impl CuDFCount {
    pub(crate) fn count_all() -> Self {
        Self { count_nulls: true }
    }
}

impl CuDFAggregationOp for CuDFCount {
    fn num_state_columns(&self) -> usize {
        1
    }

    fn reusable_state_keys(
        &self,
        args: &[Arc<dyn PhysicalExpr>],
        input_schema: &SchemaRef,
    ) -> Result<Vec<(ReusableStateKey, usize)>> {
        if self.count_nulls {
            return Ok(vec![(ReusableStateKey::CountStar, 0)]);
        }

        Ok(ExprKey::try_from_single_arg(args, input_schema)?
            .map(|key| vec![(ReusableStateKey::Count(key), 0)])
            .unwrap_or_default())
    }

    fn partial_requests(&self, args: &[CuDFColumnView]) -> Result<Vec<AggregationRequest>> {
        if args.len() != 1 {
            return exec_err!("COUNT expects 1 argument, got {}", args.len());
        }

        let mut request = AggregationRequest::from_column_view(args[0].clone());
        let op = if self.count_nulls {
            AggregationOp::COUNT_ALL
        } else {
            AggregationOp::COUNT
        };
        request.add(op.group_by());
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

    fn finalize(
        &self,
        state_cols: &[CuDFColumnView],
        _output_type: &DataType,
    ) -> Result<CuDFColumnView> {
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
