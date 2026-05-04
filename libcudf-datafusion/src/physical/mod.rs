use crate::aggregate::CuDFAggregateExec;
use arrow::array::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion_physical_plan::metrics::BaselineMetrics;
use datafusion_physical_plan::ExecutionPlan;
use std::task::Poll;

mod coalesce_batches;
mod cudf_load;
mod cudf_unload;
mod filter;
mod hash_join;
mod projection;
mod sort;

pub use coalesce_batches::CuDFCoalesceBatchesExec;
pub use cudf_load::CuDFLoadExec;
pub use cudf_unload::CuDFUnloadExec;
pub use filter::CuDFFilterExec;
pub use hash_join::{try_as_cudf_hash_join, CuDFHashJoinExec};
pub use projection::CuDFProjectionExec;
pub use sort::CuDFSortExec;

/// GPU-safe replacement for [`BaselineMetrics::record_poll`].
///
/// `record_poll` internally calls `get_record_batch_memory_size` -> `Array::to_data()`,
/// which for `CuDFColumnView` triggers a full GPU->CPU copy just to measure spill size.
/// GPU data is never subject to DataFusion's host spill mechanism, so only the row
/// count is recorded.
pub(crate) fn record_gpu_poll(
    metrics: &BaselineMetrics,
    poll: Poll<Option<Result<RecordBatch, DataFusionError>>>,
) -> Poll<Option<Result<RecordBatch, DataFusionError>>> {
    if let Poll::Ready(Some(Ok(ref batch))) = poll {
        metrics.output_rows().add(batch.num_rows());
    }
    poll
}

pub fn is_cudf_plan(plan: &dyn ExecutionPlan) -> bool {
    let any = plan.as_any();
    any.is::<CuDFAggregateExec>()
        || any.is::<CuDFFilterExec>()
        || any.is::<CuDFHashJoinExec>()
        || any.is::<CuDFLoadExec>()
        || any.is::<CuDFUnloadExec>()
        || any.is::<CuDFProjectionExec>()
        || any.is::<CuDFCoalesceBatchesExec>()
        || any.is::<CuDFSortExec>()
}
