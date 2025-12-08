use crate::aggregate::CuDFAggregateExec;
use datafusion_physical_plan::ExecutionPlan;

mod coalesce_batches;
mod cudf_load;
mod cudf_unload;
mod filter;
mod projection;
mod sort;

pub use coalesce_batches::CuDFCoalesceBatchesExec;
pub use cudf_load::CuDFLoadExec;
pub use cudf_unload::CuDFUnloadExec;
pub use filter::CuDFFilterExec;
pub use projection::CuDFProjectionExec;

pub fn is_cudf_plan(plan: &dyn ExecutionPlan) -> bool {
    let any = plan.as_any();
    any.is::<CuDFAggregateExec>()
        || any.is::<CuDFFilterExec>()
        || any.is::<CuDFLoadExec>()
        || any.is::<CuDFUnloadExec>()
        || any.is::<CuDFProjectionExec>()
        || any.is::<CuDFCoalesceBatchesExec>()
}
