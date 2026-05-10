//! DataFusion integration for GPU-accelerated query execution with cuDF
//!
//! This crate provides custom ExecutionPlan nodes that execute operations
//! on the GPU using NVIDIA's cuDF library through DataFusion.

mod decimal;
mod errors;
mod expr;
mod metrics;
mod physical;
mod planner;

#[cfg(any(feature = "integration", test))]
pub mod test_utils;

pub use physical::aggregate;
pub use physical::{CuDFLoadExec, CuDFUnloadExec};
pub use planner::{CuDFConfig, SessionStateBuilderExt};
