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
        /// Create an empty table
        fn create_empty_table() -> UniquePtr<Table>;

        /// Read a CSV file into a table
        ///
        /// # Arguments
        /// * `filename` - Path to the CSV file
        ///
        /// # Errors
        /// Returns an error if the file cannot be read or parsed
        fn read_csv(filename: &str) -> Result<UniquePtr<Table>>;

        /// Write a table to a CSV file
        ///
        /// # Arguments
        /// * `table` - The table to write
        /// * `filename` - Path to the output CSV file
        ///
        /// # Errors
        /// Returns an error if the file cannot be written
        fn write_csv(table: &Table, filename: &str) -> Result<()>;

        /// Get cuDF version information
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
    }
}
