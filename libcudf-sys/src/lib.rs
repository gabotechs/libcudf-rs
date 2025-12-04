//! Low-level FFI bindings to libcudf using cxx
//!
//! This crate provides unsafe bindings to the cuDF C++ library.
//! For a safe, idiomatic Rust API, use the `libcudf-rs` crate instead.

#[cxx::bridge(namespace = "libcudf_bridge")]
pub mod ffi {
    // Opaque C++ types
    unsafe extern "C++" {
        // Include individual headers - order matters for dependencies
        include!("libcudf-sys/src/data_type.h");
        include!("libcudf-sys/src/column.h");
        include!("libcudf-sys/src/scalar.h");
        include!("libcudf-sys/src/table.h");
        include!("libcudf-sys/src/aggregation.h");
        include!("libcudf-sys/src/groupby.h");
        include!("libcudf-sys/src/io.h");
        include!("libcudf-sys/src/operations.h");
        include!("libcudf-sys/src/binaryop.h");
        include!("libcudf-sys/src/sorting.h");

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

        /// cuDF data type with type_id and optional scale
        ///
        /// Represents a data type in cuDF, including the type_id and scale for fixed_point types.
        type DataType;

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
        fn aggregate(
            self: &GroupBy,
            requests: &[*const AggregationRequest],
        ) -> Result<UniquePtr<GroupByResult>>;

        /// Request for groupby aggregation(s) to perform on a column
        ///
        /// The group membership of each value is determined by the corresponding row
        /// in the original order of keys used to construct the groupby. Contains a column
        /// of values to aggregate and a set of aggregations to perform on those elements.
        type AggregationRequest;

        // AggregationRequest methods

        /// Add an aggregation to this request
        fn add(self: &AggregationRequest, agg: UniquePtr<Aggregation>);

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
        fn get_result_mut(
            self: Pin<&mut GroupByResult>,
            index: usize,
        ) -> Pin<&mut AggregationResult>;

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
        fn select(self: &TableView, column_indices: &[i32]) -> UniquePtr<TableView>;

        /// Get column view at index
        fn column(self: &TableView, index: i32) -> UniquePtr<ColumnView>;

        /// Get the table view schema as an FFI ArrowSchema
        unsafe fn to_arrow_schema(self: &TableView, out_schema_ptr: *mut u8) -> Result<()>;

        /// Get the table view data as an FFI ArrowArray
        unsafe fn to_arrow_array(self: &TableView, out_array_ptr: *mut u8) -> Result<()>;

        // Column methods
        /// Get the number of elements in the column
        fn size(self: &Column) -> usize;

        /// Get the data type of the column
        fn data_type(self: &Column) -> UniquePtr<DataType>;

        // ColumnView methods
        /// Get the number of elements in the column view
        fn size(self: &ColumnView) -> usize;

        /// Get a view of this column
        fn view(self: &Column) -> UniquePtr<ColumnView>;

        /// Get the column view data as an FFI ArrowArray
        unsafe fn to_arrow_array(self: &ColumnView, out_array_ptr: *mut u8) -> Result<()>;

        /// Get the raw device pointer to the column view's data
        fn data_ptr(self: &ColumnView) -> u64;

        /// Get the data type of the column view
        fn data_type(self: &ColumnView) -> UniquePtr<DataType>;

        /// Clone this column view
        fn clone(self: &ColumnView) -> UniquePtr<ColumnView>;

        // DataType methods
        /// Get the type_id
        fn id(self: &DataType) -> i32;

        /// Get the scale (for fixed_point types)
        fn scale(self: &DataType) -> i32;

        // Scalar methods
        /// Check if the scalar is valid (not null)
        fn is_valid(self: &Scalar) -> bool;

        /// Get the data type of the scalar
        fn data_type(self: &Scalar) -> UniquePtr<DataType>;

        /// Clone this scalar (deep copy)
        fn clone(self: &Scalar) -> UniquePtr<Scalar>;

        // Factory functions

        /// Create an empty table with no columns and no rows
        fn create_empty_table() -> UniquePtr<Table>;

        /// Create a table from a set of column pointers (takes ownership)
        /// The columns are consumed and should not be used after this call
        fn create_table_from_columns_move(columns: &[*mut Column]) -> UniquePtr<Table>;

        /// Create a table from vertically concatenating TableView together
        fn concat_table_views(views: &[UniquePtr<TableView>]) -> UniquePtr<Table>;

        /// Create a table from vertically concatenating ColumnView together
        fn concat_column_views(views: &[UniquePtr<ColumnView>]) -> UniquePtr<Column>;

        /// Create a TableView from a set of ColumnView pointers (non-owning)
        fn create_table_view_from_column_views(
            column_views: &[*const ColumnView],
        ) -> UniquePtr<TableView>;

        // Parquet I/O

        /// Read a Parquet file into a table
        fn read_parquet(filename: &str) -> Result<UniquePtr<Table>>;

        /// Write a table to a Parquet file
        fn write_parquet(table: &TableView, filename: &str) -> Result<()>;

        // Direct cuDF operations

        /// Filters a table using a boolean mask
        ///
        /// Given an input table and a mask column, an element `i` from each column of the input
        /// is copied to the corresponding output column if the corresponding element `i` in the
        /// mask is non-null and `true`. This operation is stable: the input order is preserved.
        fn apply_boolean_mask(
            table: &TableView,
            boolean_mask: &ColumnView,
        ) -> Result<UniquePtr<Table>>;

        // Binary operations - direct cuDF mappings

        /// Perform a binary operation between two columns
        ///
        /// Returns a new column containing the result of `op(lhs[i], rhs[i])` for all elements.
        /// The output type must be specified explicitly.
        fn binary_operation_col_col(
            lhs: &ColumnView,
            rhs: &ColumnView,
            op: i32,
            output_type_id: i32,
        ) -> Result<UniquePtr<Column>>;

        /// Perform a binary operation between a column and a scalar
        ///
        /// Returns a new column containing the result of `op(lhs[i], rhs)` for all elements.
        /// The output type must be specified explicitly.
        fn binary_operation_col_scalar(
            lhs: &ColumnView,
            rhs: &Scalar,
            op: i32,
            output_type_id: i32,
        ) -> Result<UniquePtr<Column>>;

        /// Perform a binary operation between a scalar and a column
        ///
        /// Returns a new column containing the result of `op(lhs, rhs[i])` for all elements.
        /// The output type must be specified explicitly.
        fn binary_operation_scalar_col(
            lhs: &Scalar,
            rhs: &ColumnView,
            op: i32,
            output_type_id: i32,
        ) -> Result<UniquePtr<Column>>;

        // Sorting operations - direct cuDF mappings

        /// Sort a table in lexicographic order
        ///
        /// Sorts the rows of the table according to the specified column orders and null precedence.
        fn sort_table(
            input: &TableView,
            column_order: &[i32],
            null_precedence: &[i32],
        ) -> Result<UniquePtr<Table>>;

        /// Stable sort a table in lexicographic order
        ///
        /// Like sort_table but guarantees that equivalent elements preserve their original order.
        fn stable_sort_table(
            input: &TableView,
            column_order: &[i32],
            null_precedence: &[i32],
        ) -> Result<UniquePtr<Table>>;

        /// Get the indices that would sort a table
        ///
        /// Returns a column of indices that would produce a sorted table if used to reorder the rows.
        fn sorted_order(
            input: &TableView,
            column_order: &[i32],
            null_precedence: &[i32],
        ) -> Result<UniquePtr<Column>>;

        /// Check if a table is sorted
        ///
        /// Returns true if the rows are sorted according to the specified column orders.
        fn is_sorted(
            input: &TableView,
            column_order: &[i32],
            null_precedence: &[i32],
        ) -> Result<bool>;

        /// Sort values table based on keys table
        ///
        /// Reorders the rows of `values` according to the lexicographic ordering of the rows of `keys`.
        /// The `column_order` and `null_precedence` vectors must match the number of columns in `keys`.
        fn sort_by_key(
            values: &TableView,
            keys: &TableView,
            column_order: &[i32],
            null_precedence: &[i32],
        ) -> Result<UniquePtr<Table>>;

        /// Stable sort values table based on keys table
        ///
        /// Same as `sort_by_key` but preserves the relative order of equivalent elements.
        fn stable_sort_by_key(
            values: &TableView,
            keys: &TableView,
            column_order: &[i32],
            null_precedence: &[i32],
        ) -> Result<UniquePtr<Table>>;

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
        fn reduce(
            col: &Column,
            agg: &Aggregation,
            output_type_id: i32,
        ) -> Result<UniquePtr<Scalar>>;

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

        /// Convert an Arrow DeviceArray to a cuDF table
        unsafe fn table_from_arrow_host(
            schema_ptr: *const u8,
            device_array_ptr: *const u8,
        ) -> Result<UniquePtr<Table>>;

        /// Convert an Arrow array to a cuDF column
        unsafe fn column_from_arrow(
            schema_ptr: *const u8,
            array_ptr: *const u8,
        ) -> Result<UniquePtr<Column>>;

        /// Extract a scalar from a column at the specified index
        fn get_element(column: &ColumnView, index: usize) -> UniquePtr<Scalar>;

        /// Get the version of the cuDF library
        fn get_cudf_version() -> String;
    }
}

/// Sort order for columns
///
/// Specifies whether columns should be sorted in ascending or descending order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum Order {
    /// Sort from smallest to largest
    Ascending = 0,
    /// Sort from largest to smallest
    Descending = 1,
}

/// Null ordering for sorting
///
/// Specifies whether null values should appear before or after non-null values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum NullOrder {
    /// Nulls appear after all other values
    After = 0,
    /// Nulls appear before all other values
    Before = 1,
}

/// Binary operators supported by cuDF
///
/// These operators can be used with binary_operation functions to perform
/// element-wise operations on columns and scalars.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum BinaryOperator {
    /// Addition (+)
    Add = 0,
    /// Subtraction (-)
    Sub = 1,
    /// Multiplication (*)
    Mul = 2,
    /// Division (/)
    Div = 3,
    /// True division (promotes to floating point)
    TrueDiv = 4,
    /// Floor division (//)
    FloorDiv = 5,
    /// Modulo (%)
    Mod = 6,
    /// Positive modulo
    PMod = 7,
    /// Python-style modulo
    PyMod = 8,
    /// Power (^)
    Pow = 9,
    /// Integer power
    IntPow = 10,
    /// Logarithm to base
    LogBase = 11,
    /// Two-argument arctangent
    Atan2 = 12,
    /// Shift left (<<)
    ShiftLeft = 13,
    /// Shift right (>>)
    ShiftRight = 14,
    /// Unsigned shift right (>>>)
    ShiftRightUnsigned = 15,
    /// Bitwise AND (&)
    BitwiseAnd = 16,
    /// Bitwise OR (|)
    BitwiseOr = 17,
    /// Bitwise XOR (^)
    BitwiseXor = 18,
    /// Logical AND (&&)
    LogicalAnd = 19,
    /// Logical OR (||)
    LogicalOr = 20,
    /// Equal (==)
    Equal = 21,
    /// Not equal (!=)
    NotEqual = 22,
    /// Less than (<)
    Less = 23,
    /// Greater than (>)
    Greater = 24,
    /// Less than or equal (<=)
    LessEqual = 25,
    /// Greater than or equal (>=)
    GreaterEqual = 26,
}

/// cuDF data type IDs
///
/// These correspond to cuDF's type_id enum and are used to specify
/// the output type for binary operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum TypeId {
    /// Empty type
    Empty = 0,
    /// 8-bit signed integer
    Int8 = 1,
    /// 16-bit signed integer
    Int16 = 2,
    /// 32-bit signed integer
    Int32 = 3,
    /// 64-bit signed integer
    Int64 = 4,
    /// 8-bit unsigned integer
    Uint8 = 5,
    /// 16-bit unsigned integer
    Uint16 = 6,
    /// 32-bit unsigned integer
    Uint32 = 7,
    /// 64-bit unsigned integer
    Uint64 = 8,
    /// 32-bit floating point
    Float32 = 9,
    /// 64-bit floating point
    Float64 = 10,
    /// Boolean
    Bool8 = 11,
    /// Timestamp in days since epoch
    TimestampDays = 12,
    /// Timestamp in seconds since epoch
    TimestampSeconds = 13,
    /// Timestamp in milliseconds since epoch
    TimestampMilliseconds = 14,
    /// Timestamp in microseconds since epoch
    TimestampMicroseconds = 15,
    /// Timestamp in nanoseconds since epoch
    TimestampNanoseconds = 16,
    /// Duration in days
    DurationDays = 17,
    /// Duration in seconds
    DurationSeconds = 18,
    /// Duration in milliseconds
    DurationMilliseconds = 19,
    /// Duration in microseconds
    DurationMicroseconds = 20,
    /// Duration in nanoseconds
    DurationNanoseconds = 21,
    /// Dictionary (categorical) type with 32-bit indices
    Dictionary32 = 22,
    /// String type
    String = 23,
    /// List type
    List = 24,
    /// Decimal 32-bit
    Decimal32 = 25,
    /// Decimal 64-bit
    Decimal64 = 26,
    /// Decimal 128-bit
    Decimal128 = 27,
    /// Struct type
    Struct = 28,
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
    /// Reserved bytes for future expansion
    pub reserved: [i64; 3],
}

// Thread safety implementations for cuDF types
//
// These are safe because:
// 1. GPU memory is process-wide, not thread-local
// 2. cuDF uses proper locking internally where needed
// 3. CUDA operations are serialized per-stream
// 4. The underlying cudf::column/table are just smart pointers to GPU memory

/// SAFETY: GPU device memory can be safely transferred between threads.
/// The underlying cudf::column contains only pointers to GPU memory which is
/// process-wide. CUDA contexts are managed properly by the CUDA runtime.
unsafe impl Send for ffi::Column {}

/// SAFETY: cudf::column can be safely accessed from multiple threads concurrently.
/// Read operations on GPU memory are thread-safe.
unsafe impl Sync for ffi::Column {}

/// SAFETY: ColumnView is a non-owning view and can be sent between threads.
unsafe impl Send for ffi::ColumnView {}

/// SAFETY: ColumnView can be safely accessed from multiple threads.
unsafe impl Sync for ffi::ColumnView {}

/// SAFETY: DataType is a small value type that can be safely shared and sent between threads.
/// It only contains a type_id enum and an optional scale value.
unsafe impl Send for ffi::DataType {}

/// SAFETY: DataType is immutable and can be safely accessed from multiple threads.
unsafe impl Sync for ffi::DataType {}

/// SAFETY: GPU device memory in Table can be safely transferred between threads.
unsafe impl Send for ffi::Table {}

/// SAFETY: Table can be safely accessed from multiple threads concurrently.
unsafe impl Sync for ffi::Table {}

/// SAFETY: TableView is a non-owning view and can be sent between threads.
unsafe impl Send for ffi::TableView {}

/// SAFETY: TableView can be safely accessed from multiple threads.
unsafe impl Sync for ffi::TableView {}

/// SAFETY: Scalar contains GPU memory and can be sent between threads.
unsafe impl Send for ffi::Scalar {}

/// SAFETY: Scalar can be safely accessed from multiple threads.
unsafe impl Sync for ffi::Scalar {}

/// SAFETY: Aggregation is a configuration object with no thread-local state.
unsafe impl Send for ffi::Aggregation {}

/// SAFETY: Aggregation can be safely accessed from multiple threads.
unsafe impl Sync for ffi::Aggregation {}

/// SAFETY: GroupBy configuration can be sent between threads.
unsafe impl Send for ffi::GroupBy {}

/// SAFETY: GroupBy configuration can be accessed from multiple threads.
unsafe impl Sync for ffi::GroupBy {}

/// SAFETY: AggregationRequest configuration can be sent between threads.
unsafe impl Send for ffi::AggregationRequest {}

/// SAFETY: AggregationRequest configuration can be accessed from multiple threads.
unsafe impl Sync for ffi::AggregationRequest {}

/// SAFETY: AggregationResult contains GPU data that can be sent between threads.
unsafe impl Send for ffi::AggregationResult {}

/// SAFETY: AggregationResult can be accessed from multiple threads.
unsafe impl Sync for ffi::AggregationResult {}

/// SAFETY: GroupByResult contains GPU data that can be sent between threads.
unsafe impl Send for ffi::GroupByResult {}

/// SAFETY: GroupByResult can be accessed from multiple threads.
unsafe impl Sync for ffi::GroupByResult {}
