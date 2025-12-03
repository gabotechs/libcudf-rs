use datafusion_physical_plan::ExecutionPlan;

mod cudf_load;
mod cudf_unload;
mod filter;
mod projection;

pub use cudf_load::CuDFLoadExec;
pub use cudf_unload::CuDFUnloadExec;
pub use filter::CuDFFilterExec;
pub use projection::CuDFProjectionExec;

pub fn is_cudf_plan(plan: &dyn ExecutionPlan) -> bool {
    let any = plan.as_any();
    any.is::<CuDFFilterExec>()
        || any.is::<CuDFLoadExec>()
        || any.is::<CuDFUnloadExec>()
        || any.is::<CuDFProjectionExec>()
}
