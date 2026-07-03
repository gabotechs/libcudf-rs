//! Safe, idiomatic Rust bindings for cuDF
//!
//! This crate provides a safe wrapper around the cuDF C++ library,
//! enabling GPU-accelerated dataframe operations in Rust.
//!
//! # Examples
//!
//! ```no_run
//! use libcudf_rs::{CuDFExecutionContext, CuDFTable};
//!
//! // Read a Parquet file
//! let ctx = CuDFExecutionContext::try_new_non_blocking()?;
//! let table = ctx.execute(CuDFTable::read_parquet("data.parquet"))?;
//! println!("Loaded table with {} rows and {} columns",
//!          table.num_rows(), table.num_columns());
//!
//! // Write to Parquet
//! ctx.execute(table.into_view().write_parquet("output.parquet"))?;
//! # Ok::<(), libcudf_rs::CuDFError>(())
//! ```

mod arrow;
mod ast;
mod binary_op;
mod column;
mod column_view;
mod config;
mod data_type;
mod deferred_operation;
mod errors;
mod execution_context;
mod execution_policy;
mod group_by;
mod join;
mod keep_alive;
mod parquet;
mod pinned;
mod scalar;
mod sort;
mod stream;
mod stream_readiness;
mod table;
mod table_view;

pub use arrow::{is_cudf_array, is_cudf_record_batch, record_batch_with_schema};
pub use ast::{
    CuDFAstExpression, CuDFAstExpressionBuilder, CuDFAstNode, CuDFAstOperator,
    CuDFAstTableReference,
};
pub use binary_op::{CuDFBinaryOp, CuDFColumnViewOrScalar};
pub use column::CuDFColumn;
pub use column_view::CuDFColumnView;
pub use deferred_operation::CuDFOperation;
pub use errors::{CuDFError, Result};
pub use execution_context::CuDFExecutionContext;
pub use group_by::*;
pub use join::{
    cross_join, full_join, inner_join, left_anti_join, left_join, left_semi_join, CreateHashJoin,
    CrossJoin, CuDFHashJoin, CuDFNullEquality, CuDFStreamingJoin, EquiJoin, JoinProbe,
    LeftFilterJoin, UnmatchedBuildRows,
};
pub use parquet::{CuDFParquetReadOptions, CuDFParquetReadResult};
pub use pinned::pin_record_batch;
pub use scalar::CuDFScalar;
pub use sort::SortOrder;
use std::any::Any;
use std::sync::Arc;
pub use stream::{CuDFStream, CuDFStreamFlags};
pub use table::*;
pub use table_view::*;

/// Type-erased storage retained by non-owning cuDF views.
///
/// cuDF views point at buffers owned by tables, columns, other views, or small
/// helper containers. Holding this `Arc` is enough to keep that backing storage
/// alive for as long as the view exists.
pub(crate) type CuDFViewStorage = Arc<dyn Any + Send + Sync>;

/// Get cuDF version information
///
/// # Examples
///
/// ```
/// use libcudf_rs::version;
///
/// println!("cuDF version: {}", version());
/// ```
pub fn version() -> String {
    libcudf_sys::ffi::get_cudf_version()
}

#[cfg(test)]
pub(crate) fn execute_cudf<O>(operation: O) -> Result<O::Output>
where
    O: CuDFOperation,
{
    CuDFExecutionContext::try_new_non_blocking()?.execute(operation)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        let ver = version();
        assert!(!ver.is_empty());
    }
}
