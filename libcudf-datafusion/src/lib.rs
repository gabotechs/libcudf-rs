//! DataFusion integration for GPU-accelerated query execution with cuDF
//!
//! This crate provides custom ExecutionPlan nodes that execute operations
//! on the GPU using NVIDIA's cuDF library through DataFusion.

pub mod aggregate;
mod errors;
mod expr;
mod optimizer;
mod physical;

#[cfg(any(feature = "integration", test))]
pub mod test_utils;

pub use optimizer::{CuDFConfig, HostToCuDFRule};
