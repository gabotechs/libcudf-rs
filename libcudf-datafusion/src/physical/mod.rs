use crate::aggregate::CuDFAggregateExec;
use datafusion_physical_plan::ExecutionPlan;

mod cudf_load;
mod cudf_unload;
mod filter;
mod hash_join;
mod projection;
mod sort;

pub(crate) use cudf_load::normalize_scalar_for_cudf;
pub use cudf_load::CuDFLoadExec;
pub use cudf_unload::CuDFUnloadExec;
pub use filter::CuDFFilterExec;
pub use hash_join::{try_as_cudf_hash_join, CuDFHashJoinExec};
pub use projection::CuDFProjectionExec;
pub use sort::CuDFSortExec;

pub fn is_cudf_plan(plan: &dyn ExecutionPlan) -> bool {
    let any = plan.as_any();
    any.is::<CuDFAggregateExec>()
        || any.is::<CuDFFilterExec>()
        || any.is::<CuDFHashJoinExec>()
        || any.is::<CuDFLoadExec>()
        || any.is::<CuDFUnloadExec>()
        || any.is::<CuDFProjectionExec>()
        || any.is::<CuDFSortExec>()
}
