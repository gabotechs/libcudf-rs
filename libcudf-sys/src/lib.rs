//! Low-level FFI bindings to libcudf using cxx
//!
//! This crate provides unsafe bindings to the cuDF C++ library.
//! For a safe, idiomatic Rust API, use the `libcudf-rs` crate instead.

#[cxx::bridge(namespace = "libcudf_bridge")]
pub mod ffi {
    // Opaque C++ types
    unsafe extern "C++" {
        include!("libcudf-sys/src/bridge.h");

        /// Opaque wrapper for cuDF table
        type Table;

        /// Opaque wrapper for cuDF column
        type Column;

        // Table methods
        /// Get the number of columns in the table
        fn num_columns(self: &Table) -> usize;

        /// Get the number of rows in the table
        fn num_rows(self: &Table) -> usize;

        // Column methods
        /// Get the number of elements in the column
        fn size(self: &Column) -> usize;

        // Factory functions
        fn create_empty_table() -> UniquePtr<Table>;

        // Parquet I/O
        fn read_parquet(filename: &str) -> Result<UniquePtr<Table>>;
        fn write_parquet(table: &Table, filename: &str) -> Result<()>;

        // Table operations
        fn select_columns(table: &Table, indices: &[usize]) -> Result<UniquePtr<Table>>;
        fn get_column(table: &Table, index: usize) -> Result<UniquePtr<Column>>;
        fn filter(table: &Table, boolean_mask: &Column) -> Result<UniquePtr<Table>>;

        // Column creation
        fn create_boolean_column(data: &[bool]) -> Result<UniquePtr<Column>>;

        // Utility functions
        fn get_cudf_version() -> String;
    }
}

// Re-export for convenience
pub use ffi::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_empty_table() {
        let table = ffi::create_empty_table();
        assert_eq!(table.num_columns(), 0);
        assert_eq!(table.num_rows(), 0);
    }

    #[test]
    fn test_get_version() {
        let version = ffi::get_cudf_version();
        assert!(!version.is_empty());
        println!("cuDF version: {}", version);
    }
}
