use datafusion_physical_plan::ExecutionPlan;

pub mod aggregate;
mod coalesce;
mod cudf_load;
mod cudf_unload;
mod filter;
mod hash_join;
mod parquet_scan;
mod projection;
mod sort;

pub use aggregate::CuDFAggregateExec;
pub(crate) use coalesce::CuDFCoalescePartitionsExec;
pub use cudf_load::CuDFLoadExec;
pub(crate) use cudf_load::{cudf_schema_compatibility_map, normalize_scalar_for_cudf};
pub use cudf_unload::CuDFUnloadExec;
pub use filter::CuDFFilterExec;
pub use hash_join::{try_as_cudf_hash_join, CuDFHashJoinExec};
pub use parquet_scan::{CuDFParquetScanConfig, CuDFParquetScanExec};
pub(crate) use parquet_scan::{
    CuDFParquetSource, CuDFParquetSourceBuilder, CuDFParquetSourceError,
};
pub use projection::CuDFProjectionExec;
pub use sort::CuDFSortExec;

pub fn is_cudf_plan(plan: &dyn ExecutionPlan) -> bool {
    let any = plan.as_any();
    any.is::<CuDFAggregateExec>()
        || any.is::<CuDFFilterExec>()
        || any.is::<CuDFCoalescePartitionsExec>()
        || any.is::<CuDFHashJoinExec>()
        || any.is::<CuDFLoadExec>()
        || any.is::<CuDFParquetScanExec>()
        || any.is::<CuDFUnloadExec>()
        || any.is::<CuDFProjectionExec>()
        || any.is::<CuDFSortExec>()
}
