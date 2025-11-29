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

        /// Opaque wrapper for cuDF table_view
        type TableView;

        /// Opaque wrapper for cuDF column
        type Column;

        /// Opaque wrapper for cuDF column_view
        type ColumnView;

        /// Opaque wrapper for cuDF scalar
        type Scalar;

        /// Opaque wrapper for cuDF aggregation
        type Aggregation;

        /// Opaque wrapper for cuDF groupby
        type GroupBy;

        // GroupBy methods - direct cuDF class methods
        fn aggregate(self: &GroupBy, requests: &[*const AggregationRequest]) -> Result<UniquePtr<GroupByResult>>;

        /// Opaque wrapper for cuDF aggregation_request
        type AggregationRequest;

        // AggregationRequest methods
        fn add(self: Pin<&mut AggregationRequest>, agg: UniquePtr<Aggregation>);

        /// Opaque wrapper for cuDF aggregation_result
        type AggregationResult;

        // AggregationResult methods
        fn results_size(self: &AggregationResult) -> usize;

        /// Wrapper for groupby aggregate result pair
        type GroupByResult;

        // GroupByResult methods
        fn get_keys(self: &GroupByResult) -> &Table;
        fn results_size(self: &GroupByResult) -> usize;
        fn get_result(self: Pin<&mut GroupByResult>, index: usize) -> Pin<&mut AggregationResult>;

        // Table methods
        /// Get the number of columns in the table
        fn num_columns(self: &Table) -> usize;

        /// Get the number of rows in the table
        fn num_rows(self: &Table) -> usize;

        /// Get a view of this table
        fn view(self: &Table) -> UniquePtr<TableView>;

        // TableView methods
        /// Get the number of columns in the table view
        fn num_columns(self: &TableView) -> usize;

        /// Get the number of rows in the table view
        fn num_rows(self: &TableView) -> usize;

        /// Select specific columns by indices
        fn select(self: &TableView, column_indices: &[usize]) -> UniquePtr<TableView>;

        /// Get column view at index
        fn column(self: &TableView, index: usize) -> UniquePtr<ColumnView>;

        // Column methods
        /// Get the number of elements in the column
        fn size(self: &Column) -> usize;

        // ColumnView methods
        /// Get the number of elements in the column view
        fn size(self: &ColumnView) -> usize;

        // Scalar methods
        /// Check if the scalar is valid (not null)
        fn is_valid(self: &Scalar) -> bool;

        // Factory functions
        fn create_empty_table() -> UniquePtr<Table>;
        fn create_table_from_columns(columns: &[*mut Column]) -> UniquePtr<Table>;

        // Parquet I/O
        fn read_parquet(filename: &str) -> Result<UniquePtr<Table>>;
        fn write_parquet(table: &Table, filename: &str) -> Result<()>;

        // Direct cuDF operations
        fn apply_boolean_mask(table: &Table, boolean_mask: &Column) -> Result<UniquePtr<Table>>;

        // Aggregation factory functions - direct cuDF mappings (for reduce)
        fn make_sum_aggregation() -> UniquePtr<Aggregation>;
        fn make_min_aggregation() -> UniquePtr<Aggregation>;
        fn make_max_aggregation() -> UniquePtr<Aggregation>;
        fn make_mean_aggregation() -> UniquePtr<Aggregation>;
        fn make_count_aggregation() -> UniquePtr<Aggregation>;

        // Aggregation factory functions - direct cuDF mappings (for groupby)
        fn make_sum_aggregation_groupby() -> UniquePtr<Aggregation>;
        fn make_min_aggregation_groupby() -> UniquePtr<Aggregation>;
        fn make_max_aggregation_groupby() -> UniquePtr<Aggregation>;
        fn make_mean_aggregation_groupby() -> UniquePtr<Aggregation>;
        fn make_count_aggregation_groupby() -> UniquePtr<Aggregation>;

        // Reduction - direct cuDF mapping
        fn reduce(col: &Column, agg: &Aggregation, output_type_id: i32) -> Result<UniquePtr<Scalar>>;

        // GroupBy operations - direct cuDF mappings
        fn groupby_create(keys: &TableView) -> UniquePtr<GroupBy>;
        fn aggregation_request_create(values: &ColumnView) -> UniquePtr<AggregationRequest>;

        // Arrow interop - direct cuDF calls
        unsafe fn from_arrow_host(schema_ptr: *mut u8, device_array_ptr: *mut u8) -> Result<UniquePtr<Table>>;
        unsafe fn to_arrow_schema(table: &Table, out_schema_ptr: *mut u8) -> Result<()>;
        unsafe fn to_arrow_host_array(table: &Table, out_array_ptr: *mut u8) -> Result<()>;

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
