use crate::task_context::CuDFTaskContext;
use datafusion::execution::TaskContext;
use std::sync::Arc;

/// Helpers for cloning a [`TaskContext`] with cuDF runtime state attached.
///
/// Stream-aware execution stores per-`(segment_id, partition)` CUDA stream
/// state in a [`CuDFTaskContext`] extension. Bench harnesses and integration
/// tests should call [`CuDFExt::with_cudf_task_context`] once per query run
/// to install a fresh task context, so stream state isn't reused across
/// independent runs.
pub trait CuDFExt {
    fn with_cudf_task_context(&self) -> TaskContext;
}

impl CuDFExt for TaskContext {
    fn with_cudf_task_context(&self) -> TaskContext {
        TaskContext::new(
            self.task_id(),
            self.session_id(),
            self.session_config()
                .clone()
                .with_extension(Arc::new(CuDFTaskContext::default())),
            self.scalar_functions().clone(),
            self.aggregate_functions().clone(),
            self.window_functions().clone(),
            self.runtime_env(),
        )
    }
}
