use arrow_schema::{DataType, Field, FieldRef};
use datafusion::error::Result;
use datafusion::logical_expr::Signature;
use libcudf_rs::{AggregationRequest, CuDFColumnView};
use std::fmt::Debug;
use std::sync::Arc;

pub mod sum;

/// GPU aggregation operation trait for two-phase aggregation.
///
/// This trait defines the contract for aggregation operations that can be executed
/// on the GPU using cuDF's group-by functionality. Aggregations follow a two-phase
/// pattern to support distributed and streaming execution:
///
/// 1. **Partial Phase**: Each input batch is aggregated independently, producing
///    partial results (e.g., partial sums, counts).
///
/// 2. **Final Phase**: Partial results are concatenated and re-aggregated to produce
///    the final output. The `merge` function combines the final aggregation results
///    into a single column.
///
/// # Example
///
/// For SUM aggregation:
/// - `partial_requests`: Returns a SUM aggregation request for the input column
/// - `final_requests`: Returns a SUM aggregation request for the partial sums
/// - `merge`: Returns the single summed column (identity operation for SUM)
pub trait CuDFAggregationOp: Debug + Send + Sync {
    /// Returns the name of this aggregation operation.
    fn name(&self) -> &str;

    /// Returns the function signature defining valid input types.
    fn signature(&self) -> &Signature;

    /// Returns the output data type for the given input types.
    fn return_type(&self, args: &[DataType]) -> Result<DataType>;

    /// Returns the output field for the given input fields.
    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        let arg_types: Vec<_> = arg_fields.iter().map(|f| f.data_type()).cloned().collect();
        let data_type = self.return_type(&arg_types)?;

        Ok(Arc::new(Field::new(
            self.name(),
            data_type,
            self.is_nullable(),
        )))
    }

    /// Returns whether the aggregation result can be null.
    fn is_nullable(&self) -> bool {
        true
    }

    /// Creates cuDF aggregation requests for the partial phase.
    ///
    /// Takes the input columns and returns aggregation requests to compute
    /// partial results for each input batch.
    fn partial_requests(&self, args: &[CuDFColumnView]) -> Result<Vec<AggregationRequest>>;

    /// Creates cuDF aggregation requests for the final phase.
    ///
    /// Takes the concatenated partial result columns and returns aggregation
    /// requests to compute the final aggregation.
    fn final_requests(&self, args: &[CuDFColumnView]) -> Result<Vec<AggregationRequest>>;

    /// Merges the final aggregation result columns into a single output column.
    ///
    /// For most aggregations this is an identity operation (returning the single
    /// result column), but some aggregations may need to combine multiple columns.
    fn merge(&self, args: &[CuDFColumnView]) -> Result<CuDFColumnView>;
}
