pub mod clickbench;
mod common;
pub mod tpcds;
pub mod tpch;

pub use common::{apply_query_settings, register_tables};
