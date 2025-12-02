//! Safe, idiomatic Rust bindings for cuDF
//!
//! This crate provides a safe wrapper around the cuDF C++ library,
//! enabling GPU-accelerated dataframe operations in Rust.
//!
//! # Examples
//!
//! ```no_run
//! use libcudf_rs::CuDFTable;
//!
//! // Read a Parquet file
//! let table = CuDFTable::from_parquet("data.parquet").expect("Failed to read Parquet");
//! println!("Loaded table with {} rows and {} columns",
//!          table.num_rows(), table.num_columns());
//!
//! // Write to Parquet
//! table.to_parquet("output.parquet").expect("Failed to write Parquet");
//! ```

mod column;
mod data_type;
mod errors;
mod table;

pub use column::CuDFColumn;
pub use data_type::*;
pub use errors::{CuDFError, Result};
pub use libcudf_sys::*;
pub use table::CuDFTable;

/// Get cuDF version information
///
/// # Examples
///
/// ```
/// use libcudf_rs::version;
///
/// println!("cuDF version: {}", version());
/// ```
pub fn version() -> String {
    ffi::get_cudf_version()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        let ver = version();
        assert!(!ver.is_empty());
    }
}
