//! DataFusion integration for GPU-accelerated query execution with cuDF
//!
//! This crate provides custom ExecutionPlan nodes that execute operations
//! on the GPU using NVIDIA's cuDF library through DataFusion.

pub mod aggregate;
mod cudf_ext;
mod decimal;
mod errors;
mod expr;
mod metrics;
mod physical;
mod planner;
mod stream_source;
mod task_context;

#[cfg(any(feature = "integration", test))]
pub mod test_utils;

pub use cudf_ext::CuDFExt;
pub use physical::{CuDFLoadExec, CuDFUnloadExec};
pub use planner::{CuDFConfig, SessionStateBuilderExt};
pub use task_context::CuDFTaskContext;
