//! Low-level FFI bindings to libcudf using cxx
//!
//! This crate provides unsafe bindings to the cuDF C++ library.
//! For a safe, idiomatic Rust API, use the `libcudf-rs` crate instead.

#[cxx::bridge(namespace = "libcudf_bridge")]
pub mod ffi {
    // Opaque C++ types
    unsafe extern "C++" {
        include!("libcudf-sys/src/bridge.h");

        /// A set of cuDF columns of the same size
        ///
        /// This is an owning type that represents a table in cuDF. A table is a collection
        /// of columns with the same number of rows.
        type Table;

        /// Non-owning view of a table
        ///
        /// A table_view is a set of column_views of equal size. It is non-owning and
        /// trivially copyable, providing a view into table data without owning it.
        type TableView;

        /// A container of nullable device data as a column of elements
        ///
        /// This is an owning type that represents a column of data in cuDF. Columns can
        /// contain null values and have an associated data type.
        type Column;

        /// Non-owning view of a column
        ///
        /// A column_view is a non-owning, immutable view of device data as a column of elements,
        /// some of which may be null as indicated by a bitmask.
        type ColumnView;

        /// An owning class to represent a singular value
        ///
        /// A scalar is a singular value of any of the supported data types in cuDF.
        /// Scalars can be valid or null.
        type Scalar;

        /// Abstract base class for specifying aggregation operations
        ///
        /// Represents the desired aggregation in an aggregation_request. Different aggregation
        /// types (SUM, MIN, MAX, MEAN, COUNT, etc.) are created using factory functions.
        type Aggregation;

        /// Groups values by keys and computes aggregations on those groups
        ///
        /// The groupby object is constructed with a set of key columns and can perform
        /// various aggregations on value columns based on those keys.
        type GroupBy;

        // GroupBy methods - direct cuDF class methods

        /// Performs grouped aggregations on the specified values
        ///
        /// For each aggregation in a request, `values[i]` is aggregated with all other
        /// `values[j]` where rows `i` and `j` in `keys` are equivalent.
        fn aggregate(self: &GroupBy, requests: &[*const AggregationRequest]) -> Result<UniquePtr<GroupByResult>>;

        /// Request for groupby aggregation(s) to perform on a column
        ///
        /// The group membership of each value is determined by the corresponding row
        /// in the original order of keys used to construct the groupby. Contains a column
        /// of values to aggregate and a set of aggregations to perform on those elements.
        type AggregationRequest;

        // AggregationRequest methods

        /// Add an aggregation to this request
        fn add(self: Pin<&mut AggregationRequest>, agg: UniquePtr<Aggregation>);

        /// The result(s) of an aggregation_request
        ///
        /// For every aggregation_request given to groupby::aggregate, an aggregation_result
        /// will be returned. The aggregation_result holds the resulting column(s) for each
        /// requested aggregation on the request's values.
        ///
        /// Note: Due to cxx limitations, the internal rust::Vec<Column> cannot be exposed
        /// directly. Use len() and index access via [] to work with results.
        type AggregationResult;

        /// Get the number of result columns
        fn len(self: &AggregationResult) -> usize;

        /// Get a reference to the column at the specified index
        fn get(self: &AggregationResult, index: usize) -> &Column;

        /// Result pair from a groupby aggregation operation
        ///
        /// Contains both the group keys (unique combinations of key column values) and
        /// the aggregation results for each request. The keys table identifies each group,
        /// and the results contain the computed aggregations for each group.
        type GroupByResult;

        // GroupByResult methods

        /// Get the group labels for each group
        fn get_keys(self: &GroupByResult) -> &Table;

        /// Get the number of aggregation requests
        fn results_size(self: &GroupByResult) -> usize;

        /// Get an immutable reference to the aggregation result at the specified index
        fn get_result(self: &GroupByResult, index: usize) -> &AggregationResult;

        /// Get a mutable reference to the aggregation result at the specified index
        fn get_result_mut(self: Pin<&mut GroupByResult>, index: usize) -> Pin<&mut AggregationResult>;

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

        /// Create an empty table with no columns and no rows
        fn create_empty_table() -> UniquePtr<Table>;

        /// Create a table from a set of columns
        fn create_table_from_columns(columns: &[*mut Column]) -> UniquePtr<Table>;

        // Parquet I/O

        /// Read a Parquet file into a table
        fn read_parquet(filename: &str) -> Result<UniquePtr<Table>>;

        /// Write a table to a Parquet file
        fn write_parquet(table: &Table, filename: &str) -> Result<()>;

        // Direct cuDF operations

        /// Filters a table using a boolean mask
        ///
        /// Given an input table and a mask column, an element `i` from each column of the input
        /// is copied to the corresponding output column if the corresponding element `i` in the
        /// mask is non-null and `true`. This operation is stable: the input order is preserved.
        fn apply_boolean_mask(table: &Table, boolean_mask: &Column) -> Result<UniquePtr<Table>>;

        // Aggregation factory functions - direct cuDF mappings (for reduce)

        /// Create a SUM aggregation
        fn make_sum_aggregation() -> UniquePtr<Aggregation>;

        /// Create a MIN aggregation
        fn make_min_aggregation() -> UniquePtr<Aggregation>;

        /// Create a MAX aggregation
        fn make_max_aggregation() -> UniquePtr<Aggregation>;

        /// Create a MEAN aggregation
        fn make_mean_aggregation() -> UniquePtr<Aggregation>;

        /// Create a COUNT aggregation
        fn make_count_aggregation() -> UniquePtr<Aggregation>;

        // Aggregation factory functions - direct cuDF mappings (for groupby)

        /// Create a SUM aggregation for groupby operations
        fn make_sum_aggregation_groupby() -> UniquePtr<Aggregation>;

        /// Create a MIN aggregation for groupby operations
        fn make_min_aggregation_groupby() -> UniquePtr<Aggregation>;

        /// Create a MAX aggregation for groupby operations
        fn make_max_aggregation_groupby() -> UniquePtr<Aggregation>;

        /// Create a MEAN aggregation for groupby operations
        fn make_mean_aggregation_groupby() -> UniquePtr<Aggregation>;

        /// Create a COUNT aggregation for groupby operations
        fn make_count_aggregation_groupby() -> UniquePtr<Aggregation>;

        // Reduction - direct cuDF mapping

        /// Computes the reduction of the values in all rows of a column
        ///
        /// This function does not detect overflows in reductions. Any null values are skipped
        /// for the operation. If the reduction fails, the output scalar returns with `is_valid()==false`.
        fn reduce(col: &Column, agg: &Aggregation, output_type_id: i32) -> Result<UniquePtr<Scalar>>;

        // GroupBy operations - direct cuDF mappings

        /// Construct a groupby object with the specified keys
        ///
        /// The groupby object groups values by keys and computes aggregations on those groups.
        fn groupby_create(keys: &TableView) -> UniquePtr<GroupBy>;

        /// Create an aggregation request for a column of values
        ///
        /// The group membership of each `value[i]` is determined by the corresponding row `i`
        /// in the original order of `keys` used to construct the groupby.
        fn aggregation_request_create(values: &ColumnView) -> UniquePtr<AggregationRequest>;

        // Arrow interop - direct cuDF calls

        /// Convert an Arrow array to a cuDF table
        unsafe fn from_arrow_host(schema_ptr: *mut u8, device_array_ptr: *mut u8) -> Result<UniquePtr<Table>>;

        /// Convert a cuDF table schema to Arrow schema
        unsafe fn to_arrow_schema(table: &Table, out_schema_ptr: *mut u8) -> Result<()>;

        /// Convert a cuDF table to Arrow array
        unsafe fn to_arrow_host_array(table: &Table, out_array_ptr: *mut u8) -> Result<()>;

        // Utility functions

        /// Get the version of the cuDF library
        fn get_cudf_version() -> String;
    }
}

/// Safe wrapper for converting an Arrow array to a cuDF table
///
/// This function provides a safe interface to the underlying FFI function by
/// taking references to the Arrow FFI structures.
///
/// # Arguments
///
/// * `schema` - Reference to an Arrow FFI schema
/// * `device_array` - Reference to an Arrow device array
///
/// # Returns
///
/// A cuDF table created from the Arrow data
///
/// # Errors
///
/// Returns an error if the Arrow data cannot be converted to a cuDF table
pub fn table_from_arrow(
    schema: &arrow::ffi::FFI_ArrowSchema,
    device_array: &ArrowDeviceArray,
) -> Result<cxx::UniquePtr<ffi::Table>, cxx::Exception> {
    let schema_ptr = schema as *const arrow::ffi::FFI_ArrowSchema as *mut u8;
    let device_array_ptr = device_array as *const ArrowDeviceArray as *mut u8;
    unsafe { ffi::from_arrow_host(schema_ptr, device_array_ptr) }
}

/// Safe wrapper for converting a cuDF table to Arrow schema
///
/// This function provides a safe interface to the underlying FFI function by
/// taking a mutable reference to the Arrow FFI schema.
///
/// # Arguments
///
/// * `table` - Reference to the cuDF table
/// * `out_schema` - Mutable reference to an Arrow FFI schema to be filled
///
/// # Errors
///
/// Returns an error if the table cannot be converted to an Arrow schema
pub fn table_to_arrow_schema(
    table: &ffi::Table,
    out_schema: &mut arrow::ffi::FFI_ArrowSchema,
) -> Result<(), cxx::Exception> {
    let schema_ptr = out_schema as *mut arrow::ffi::FFI_ArrowSchema as *mut u8;
    unsafe { ffi::to_arrow_schema(table, schema_ptr) }
}

/// Safe wrapper for converting a cuDF table to Arrow array
///
/// This function provides a safe interface to the underlying FFI function by
/// taking a mutable reference to the Arrow device array.
///
/// # Arguments
///
/// * `table` - Reference to the cuDF table
/// * `out_array` - Mutable reference to an Arrow device array to be filled
///
/// # Errors
///
/// Returns an error if the table cannot be converted to an Arrow array
pub fn table_to_arrow_array(
    table: &ffi::Table,
    out_array: &mut ArrowDeviceArray,
) -> Result<(), cxx::Exception> {
    let array_ptr = out_array as *mut ArrowDeviceArray as *mut u8;
    unsafe { ffi::to_arrow_host_array(table, array_ptr) }
}

/// Arrow Device Array C ABI structure
///
/// This struct represents the Arrow C Device Data Interface structure used for
/// interop between Arrow and cuDF. It extends the standard Arrow C Data Interface
/// with device information.
///
/// # Safety
///
/// This struct must maintain the exact memory layout as defined by the Arrow C Device
/// Data Interface specification.
#[repr(C)]
pub struct ArrowDeviceArray {
    /// The Arrow array data
    pub array: arrow::ffi::FFI_ArrowArray,
    /// Device ID where the data resides (-1 for CPU)
    pub device_id: i64,
    /// Device type (1 = CPU, 2 = CUDA, etc.)
    pub device_type: i32,
    /// Synchronization event pointer (usually null)
    pub sync_event: *mut std::ffi::c_void,
}

impl ArrowDeviceArray {
    /// Create an empty ArrowDeviceArray for CPU
    pub fn empty() -> Self {
        Self {
            array: arrow::ffi::FFI_ArrowArray::empty(),
            device_id: -1,
            device_type: 1, // ARROW_DEVICE_CPU
            sync_event: std::ptr::null_mut(),
        }
    }

    /// Create an ArrowDeviceArray for CPU with the given array
    pub fn new_cpu(array: arrow::ffi::FFI_ArrowArray) -> Self {
        Self {
            array,
            device_id: -1,
            device_type: 1, // ARROW_DEVICE_CPU
            sync_event: std::ptr::null_mut(),
        }
    }

    /// Create an ArrowDeviceArray for CUDA device with the given array and device ID
    pub fn new_cuda(array: arrow::ffi::FFI_ArrowArray, device_id: i64) -> Self {
        Self {
            array,
            device_id,
            device_type: 2, // ARROW_DEVICE_CUDA
            sync_event: std::ptr::null_mut(),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_create_empty_table() {
        let table = super::ffi::create_empty_table();
        assert_eq!(table.num_columns(), 0);
        assert_eq!(table.num_rows(), 0);
    }

    #[test]
    fn test_get_version() {
        let version = super::ffi::get_cudf_version();
        assert!(!version.is_empty());
        println!("cuDF version: {}", version);
    }
}
