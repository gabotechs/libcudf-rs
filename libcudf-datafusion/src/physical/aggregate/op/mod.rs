use crate::physical::aggregate::{AggregateStatePlanner, DerivedAggregateOutput, ReusableStateKey};
use arrow_schema::DataType;
use arrow_schema::SchemaRef;
use datafusion::error::Result;
use datafusion_physical_plan::aggregates::AggregateMode;
use datafusion_physical_plan::PhysicalExpr;
use libcudf_rs::{AggregationRequest, CuDFColumn, CuDFColumnView};
use std::fmt::Debug;
use std::sync::Arc;

pub mod avg;
pub mod count;
pub mod max;
pub mod min;
pub mod sum;
pub mod udf;

/// GPU aggregation operation backed by cuDF.
///
/// Each implementation defines a three-phase lifecycle that mirrors DataFusion's
/// Partial/Final model:
///
/// 1. **partial** — aggregate raw input rows into per-group intermediate state
/// 2. **merge** — combine intermediate states (from multiple batches or partitions)
/// 3. **finalize** — convert merged state into the final output column
///
/// Column ordering in `partial_requests` must match the DataFusion UDF's
/// `state_fields()` order, since the Partial->Final schema contract is positional.
pub trait CuDFAggregationOp: Debug + Send + Sync {
    /// Number of intermediate state columns this op produces.
    /// Must match the DataFusion UDF's `state_fields().len()`.
    fn num_state_columns(&self) -> usize;

    /// Return whether this op supports the argument columns that DataFusion will
    /// pass for the given aggregate mode.
    fn supports_input_types(
        &self,
        _mode: AggregateMode,
        _input_types: &[DataType],
        _output_type: &DataType,
    ) -> bool {
        true
    }

    /// Whether this aggregate can produce its final output from reusable state
    /// published by other physical aggregates.
    fn can_derive_from_reusable_state(&self) -> bool {
        false
    }

    /// State keys published by this physical aggregate, if its output can be
    /// reused by another aggregate in the same `AggregateExec`.
    fn reusable_state_keys(
        &self,
        _args: &[Arc<dyn PhysicalExpr>],
        _input_schema: &SchemaRef,
    ) -> Result<Vec<(ReusableStateKey, usize)>> {
        Ok(vec![])
    }

    /// Build a derived output from reusable state, if the state is available.
    fn try_prepare_derived_output(
        &self,
        _args: &[Arc<dyn PhysicalExpr>],
        _output_type: &DataType,
        _input_schema: &SchemaRef,
        _state: &mut AggregateStatePlanner<'_>,
    ) -> Result<Option<DerivedAggregateOutput>> {
        Ok(None)
    }

    /// Build cuDF aggregation requests for raw input data.
    fn partial_requests(&self, args: &[CuDFColumnView]) -> Result<Vec<AggregationRequest>>;

    /// Normalize state columns produced by `partial_requests` so their types are
    /// compatible with `merge_requests` for subsequent merge cycles, and match the
    /// DataFusion UDF's `state_fields()` contract when emitting `Partial` mode output.
    ///
    /// Override when the cuDF operation used in `partial_requests` returns a narrower
    /// type than the one used in `merge_requests` (e.g. COUNT -> Int32, SUM -> Int64).
    /// Default is a no-op.
    fn normalize_partial_state(&self, cols: Vec<CuDFColumn>) -> Result<Vec<CuDFColumn>> {
        Ok(cols)
    }

    /// Build cuDF aggregation requests that combine intermediate state columns.
    fn merge_requests(&self, args: &[CuDFColumnView]) -> Result<Vec<AggregationRequest>>;

    /// Convert merged state columns into the final output column.
    fn finalize(&self, args: &[CuDFColumnView], output_type: &DataType) -> Result<CuDFColumnView>;
}
