//! Low-level FFI bindings to libcudf using cxx
//!
//! This crate provides unsafe bindings to the cuDF C++ library.
//! For a safe, idiomatic Rust API, use the `libcudf-rs` crate instead.

use arrow::ffi::FFI_ArrowArray;

/// FFI bindings to cuDF C++ library
///
/// This is a thin wrapper over cuDF C++ APIs. Safety documentation is provided
/// at the higher-level `libcudf-rs` wrapper layer where safe APIs are exposed.
///
/// The cxx macro generates code that clippy cannot see source documentation for,
/// so we allow missing_safety_doc here. All safety contracts are documented in:
/// - The C++ cuDF library headers
/// - The safe wrapper functions in `libcudf-rs`
#[allow(clippy::missing_safety_doc)]
#[cxx::bridge(namespace = "libcudf_bridge")]
pub mod ffi {
    // Opaque C++ types
    unsafe extern "C++" {
        // Include individual headers - order matters for dependencies
        include!("libcudf-sys/src/data_type.h");
        include!("libcudf-sys/src/column.h");
        include!("libcudf-sys/src/scalar.h");
        include!("libcudf-sys/src/ast.h");
        include!("libcudf-sys/src/table.h");
        include!("libcudf-sys/src/interop.h");
        include!("libcudf-sys/src/pinned_memory.h");
        include!("libcudf-sys/src/aggregation.h");
        include!("libcudf-sys/src/groupby.h");
        include!("libcudf-sys/src/io.h");
        include!("libcudf-sys/src/concatenate.h");
        include!("libcudf-sys/src/copying.h");
        include!("libcudf-sys/src/filling.h");
        include!("libcudf-sys/src/stream_compaction.h");
        include!("libcudf-sys/src/version.h");
        include!("libcudf-sys/src/binaryop.h");
        include!("libcudf-sys/src/sorting.h");
        include!("libcudf-sys/src/join.h");
        include!("libcudf-sys/src/stream.h");
        include!("libcudf-sys/src/memory_resource.h");
        include!("libcudf-sys/src/pinned_host.h");
        include!("libcudf-sys/src/device_memory.h");

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

        /// Non-owning reference to a column owned by a table.
        ///
        /// This is the minimal cxx wrapper for `cudf::column const&` and must
        /// not outlive its owning [`Table`].
        type ColumnRef;

        /// Non-owning view of a column
        ///
        /// A column_view is a non-owning, immutable view of device data as a column of elements,
        /// some of which may be null as indicated by a bitmask.
        type ColumnView;

        /// Owning bridge container for `std::vector<cudf::column_view>`.
        type ColumnViews;

        /// Return the number of column views.
        fn len(self: &ColumnViews) -> usize;

        /// Return whether the container has no column views.
        fn is_empty(self: &ColumnViews) -> bool;

        /// Copy one non-owning column view from the container.
        ///
        /// # Safety
        ///
        /// The result must not outlive the data owner from which the views were sliced.
        unsafe fn get(self: &ColumnViews, index: usize) -> Result<UniquePtr<ColumnView>>;

        /// Owning bridge container for `std::vector<cudf::table_view>`.
        type TableViews;

        /// Return the number of table views.
        fn len(self: &TableViews) -> usize;

        /// Return whether the container has no table views.
        fn is_empty(self: &TableViews) -> bool;

        /// Copy one non-owning table view from the container.
        ///
        /// # Safety
        ///
        /// The result must not outlive the data owner from which the views were sliced.
        unsafe fn get(self: &TableViews, index: usize) -> Result<UniquePtr<TableView>>;

        /// cuDF data type with type_id and optional scale
        ///
        /// Represents a data type in cuDF, including the type_id and scale for fixed_point types.
        type DataType;

        /// An owning class to represent a singular value
        ///
        /// A scalar is a singular value of any of the supported data types in cuDF.
        /// Scalars can be valid or null.
        type Scalar;

        /// Direct wrapper for `cudf::column_metadata`.
        type ColumnMetadata;

        /// Direct wrapper for `std::vector<cudf::column_metadata>`.
        type ColumnMetadataVector;

        /// Return the number of metadata entries.
        fn len(self: &ColumnMetadataVector) -> usize;

        /// Return whether there are no metadata entries.
        fn is_empty(self: &ColumnMetadataVector) -> bool;

        /// Return the metadata entry at `index` by value.
        fn get(self: &ColumnMetadataVector, index: usize) -> Result<UniquePtr<ColumnMetadata>>;

        /// Replace the metadata entry at `index`.
        fn set(
            self: Pin<&mut ColumnMetadataVector>,
            index: usize,
            metadata: &ColumnMetadata,
        ) -> Result<()>;

        /// Append one metadata entry.
        fn push(self: Pin<&mut ColumnMetadataVector>, metadata: &ColumnMetadata);

        /// Return the column name.
        fn name(self: &ColumnMetadata) -> String;

        /// Replace the column name.
        fn set_name(self: Pin<&mut ColumnMetadata>, value: &str);

        /// Return the column timezone.
        fn timezone(self: &ColumnMetadata) -> String;

        /// Replace the column timezone.
        fn set_timezone(self: Pin<&mut ColumnMetadata>, value: &str);

        /// Return whether decimal precision is present.
        fn has_precision(self: &ColumnMetadata) -> bool;

        /// Return decimal precision, or an error if it is absent.
        fn precision(self: &ColumnMetadata) -> Result<i32>;

        /// Set decimal precision.
        fn set_precision(self: Pin<&mut ColumnMetadata>, value: i32);

        /// Clear decimal precision.
        fn clear_precision(self: Pin<&mut ColumnMetadata>);

        /// Return the number of recursive child metadata entries.
        fn children_len(self: &ColumnMetadata) -> usize;

        /// Return a recursive child metadata entry by value.
        fn child(self: &ColumnMetadata, index: usize) -> Result<UniquePtr<ColumnMetadata>>;

        /// Replace a recursive child metadata entry.
        fn set_child(
            self: Pin<&mut ColumnMetadata>,
            index: usize,
            child: &ColumnMetadata,
        ) -> Result<()>;

        /// Append a recursive child metadata entry.
        fn push_child(self: Pin<&mut ColumnMetadata>, child: &ColumnMetadata);

        /// Construct `cudf::column_metadata()`.
        fn make_column_metadata() -> UniquePtr<ColumnMetadata>;

        /// Construct `cudf::column_metadata(name)`.
        fn make_column_metadata_with_name(name: &str) -> UniquePtr<ColumnMetadata>;

        /// Construct an empty `std::vector<cudf::column_metadata>`.
        fn make_column_metadata_vector() -> UniquePtr<ColumnMetadataVector>;

        /// Owning cuDF AST expression tree.
        type AstExpressionTree;

        /// cuDF source information for IO readers.
        type SourceInfo;

        /// cuDF sink information for IO writers.
        type SinkInfo;

        /// cuDF Parquet reader options.
        type ParquetReaderOptions;

        /// cuDF chunked Parquet reader.
        type ChunkedParquetReader;

        /// cuDF Parquet writer options.
        type ParquetWriterOptions;

        /// cuDF table and metadata returned by I/O readers.
        type TableWithMetadata;

        /// Recursive Parquet schema information for one column.
        type ColumnNameInfo;

        /// Parquet metadata key/value entries.
        type KeyValuePairs;

        /// Complete metadata returned by a cuDF I/O reader.
        type TableMetadata;

        /// Host byte vector returned by Parquet writers for file metadata.
        type HostByteVector;

        /// Aggregation operation accepted by cuDF reductions.
        type ReduceAggregation;

        /// Aggregation operation accepted by cuDF groupby aggregation requests.
        type GroupByAggregation;

        /// Groups values by keys and computes aggregations on those groups
        ///
        /// The groupby object is constructed with a set of key columns and can perform
        /// various aggregations on value columns based on those keys.
        type GroupBy;

        /// Reusable cuDF hash join object.
        ///
        /// Builds a hash table once and probes it with subsequent join calls.
        type HashJoin;

        /// Reusable cuDF filtered join object.
        type FilteredJoin;

        /// Owning device vector of cuDF row indices.
        type DeviceIndexVector;

        /// Return the number of row indices.
        fn size(self: &DeviceIndexVector) -> usize;

        /// View the row indices as a non-owning cuDF column view.
        ///
        /// # Safety
        ///
        /// The returned view must not outlive this vector.
        unsafe fn view(self: &DeviceIndexVector) -> UniquePtr<ColumnView>;

        /// Pair of cuDF join index maps.
        type JoinIndices;

        /// Take the left input indices from this join result.
        fn release_left(self: Pin<&mut JoinIndices>) -> UniquePtr<DeviceIndexVector>;

        /// Take the right input indices from this join result.
        fn release_right(self: Pin<&mut JoinIndices>) -> UniquePtr<DeviceIndexVector>;

        /// Pair of reusable hash-join probe/build index maps.
        type HashJoinIndices;

        /// Take the probe-side indices from this reusable hash join result.
        fn release_probe(self: Pin<&mut HashJoinIndices>) -> UniquePtr<DeviceIndexVector>;

        /// Take the build-side indices from this reusable hash join result.
        fn release_build(self: Pin<&mut HashJoinIndices>) -> UniquePtr<DeviceIndexVector>;

        /// Create an empty cuDF AST expression tree.
        fn ast_expression_tree_create() -> UniquePtr<AstExpressionTree>;

        /// Add a column reference expression to an AST tree.
        fn ast_expression_tree_add_column_reference(
            tree: Pin<&mut AstExpressionTree>,
            column_index: i32,
            table_reference: i32,
        ) -> Result<usize>;

        /// Add a column-name reference expression to an AST tree.
        fn ast_expression_tree_add_column_name_reference(
            tree: Pin<&mut AstExpressionTree>,
            column_name: &str,
        ) -> Result<usize>;

        /// Add an INT8 scalar literal expression to an AST tree.
        ///
        /// # Safety
        ///
        /// The scalar's underlying cuDF allocation must outlive `tree`.
        unsafe fn ast_expression_tree_add_literal_int8(
            tree: Pin<&mut AstExpressionTree>,
            scalar: &Scalar,
        ) -> Result<usize>;
        /// Add an INT16 scalar literal expression to an AST tree.
        ///
        /// # Safety
        ///
        /// The scalar's underlying cuDF allocation must outlive `tree`.
        unsafe fn ast_expression_tree_add_literal_int16(
            tree: Pin<&mut AstExpressionTree>,
            scalar: &Scalar,
        ) -> Result<usize>;
        /// Add an INT32 scalar literal expression to an AST tree.
        ///
        /// # Safety
        ///
        /// The scalar's underlying cuDF allocation must outlive `tree`.
        unsafe fn ast_expression_tree_add_literal_int32(
            tree: Pin<&mut AstExpressionTree>,
            scalar: &Scalar,
        ) -> Result<usize>;
        /// Add an INT64 scalar literal expression to an AST tree.
        ///
        /// # Safety
        ///
        /// The scalar's underlying cuDF allocation must outlive `tree`.
        unsafe fn ast_expression_tree_add_literal_int64(
            tree: Pin<&mut AstExpressionTree>,
            scalar: &Scalar,
        ) -> Result<usize>;
        /// Add a UINT8 scalar literal expression to an AST tree.
        ///
        /// # Safety
        ///
        /// The scalar's underlying cuDF allocation must outlive `tree`.
        unsafe fn ast_expression_tree_add_literal_uint8(
            tree: Pin<&mut AstExpressionTree>,
            scalar: &Scalar,
        ) -> Result<usize>;
        /// Add a UINT16 scalar literal expression to an AST tree.
        ///
        /// # Safety
        ///
        /// The scalar's underlying cuDF allocation must outlive `tree`.
        unsafe fn ast_expression_tree_add_literal_uint16(
            tree: Pin<&mut AstExpressionTree>,
            scalar: &Scalar,
        ) -> Result<usize>;
        /// Add a UINT32 scalar literal expression to an AST tree.
        ///
        /// # Safety
        ///
        /// The scalar's underlying cuDF allocation must outlive `tree`.
        unsafe fn ast_expression_tree_add_literal_uint32(
            tree: Pin<&mut AstExpressionTree>,
            scalar: &Scalar,
        ) -> Result<usize>;
        /// Add a UINT64 scalar literal expression to an AST tree.
        ///
        /// # Safety
        ///
        /// The scalar's underlying cuDF allocation must outlive `tree`.
        unsafe fn ast_expression_tree_add_literal_uint64(
            tree: Pin<&mut AstExpressionTree>,
            scalar: &Scalar,
        ) -> Result<usize>;
        /// Add a FLOAT32 scalar literal expression to an AST tree.
        ///
        /// # Safety
        ///
        /// The scalar's underlying cuDF allocation must outlive `tree`.
        unsafe fn ast_expression_tree_add_literal_float32(
            tree: Pin<&mut AstExpressionTree>,
            scalar: &Scalar,
        ) -> Result<usize>;
        /// Add a FLOAT64 scalar literal expression to an AST tree.
        ///
        /// # Safety
        ///
        /// The scalar's underlying cuDF allocation must outlive `tree`.
        unsafe fn ast_expression_tree_add_literal_float64(
            tree: Pin<&mut AstExpressionTree>,
            scalar: &Scalar,
        ) -> Result<usize>;
        /// Add a BOOL8 scalar literal expression to an AST tree.
        ///
        /// # Safety
        ///
        /// The scalar's underlying cuDF allocation must outlive `tree`.
        unsafe fn ast_expression_tree_add_literal_bool8(
            tree: Pin<&mut AstExpressionTree>,
            scalar: &Scalar,
        ) -> Result<usize>;
        /// Add a STRING scalar literal expression to an AST tree.
        ///
        /// # Safety
        ///
        /// The scalar's underlying cuDF allocation must outlive `tree`.
        unsafe fn ast_expression_tree_add_literal_string(
            tree: Pin<&mut AstExpressionTree>,
            scalar: &Scalar,
        ) -> Result<usize>;
        /// Add a day timestamp scalar literal expression to an AST tree.
        ///
        /// # Safety
        ///
        /// The scalar's underlying cuDF allocation must outlive `tree`.
        unsafe fn ast_expression_tree_add_literal_timestamp_days(
            tree: Pin<&mut AstExpressionTree>,
            scalar: &Scalar,
        ) -> Result<usize>;
        /// Add a second timestamp scalar literal expression to an AST tree.
        ///
        /// # Safety
        ///
        /// The scalar's underlying cuDF allocation must outlive `tree`.
        unsafe fn ast_expression_tree_add_literal_timestamp_seconds(
            tree: Pin<&mut AstExpressionTree>,
            scalar: &Scalar,
        ) -> Result<usize>;
        /// Add a millisecond timestamp scalar literal expression to an AST tree.
        ///
        /// # Safety
        ///
        /// The scalar's underlying cuDF allocation must outlive `tree`.
        unsafe fn ast_expression_tree_add_literal_timestamp_milliseconds(
            tree: Pin<&mut AstExpressionTree>,
            scalar: &Scalar,
        ) -> Result<usize>;
        /// Add a microsecond timestamp scalar literal expression to an AST tree.
        ///
        /// # Safety
        ///
        /// The scalar's underlying cuDF allocation must outlive `tree`.
        unsafe fn ast_expression_tree_add_literal_timestamp_microseconds(
            tree: Pin<&mut AstExpressionTree>,
            scalar: &Scalar,
        ) -> Result<usize>;
        /// Add a nanosecond timestamp scalar literal expression to an AST tree.
        ///
        /// # Safety
        ///
        /// The scalar's underlying cuDF allocation must outlive `tree`.
        unsafe fn ast_expression_tree_add_literal_timestamp_nanoseconds(
            tree: Pin<&mut AstExpressionTree>,
            scalar: &Scalar,
        ) -> Result<usize>;
        /// Add a day duration scalar literal expression to an AST tree.
        ///
        /// # Safety
        ///
        /// The scalar's underlying cuDF allocation must outlive `tree`.
        unsafe fn ast_expression_tree_add_literal_duration_days(
            tree: Pin<&mut AstExpressionTree>,
            scalar: &Scalar,
        ) -> Result<usize>;
        /// Add a second duration scalar literal expression to an AST tree.
        ///
        /// # Safety
        ///
        /// The scalar's underlying cuDF allocation must outlive `tree`.
        unsafe fn ast_expression_tree_add_literal_duration_seconds(
            tree: Pin<&mut AstExpressionTree>,
            scalar: &Scalar,
        ) -> Result<usize>;
        /// Add a millisecond duration scalar literal expression to an AST tree.
        ///
        /// # Safety
        ///
        /// The scalar's underlying cuDF allocation must outlive `tree`.
        unsafe fn ast_expression_tree_add_literal_duration_milliseconds(
            tree: Pin<&mut AstExpressionTree>,
            scalar: &Scalar,
        ) -> Result<usize>;
        /// Add a microsecond duration scalar literal expression to an AST tree.
        ///
        /// # Safety
        ///
        /// The scalar's underlying cuDF allocation must outlive `tree`.
        unsafe fn ast_expression_tree_add_literal_duration_microseconds(
            tree: Pin<&mut AstExpressionTree>,
            scalar: &Scalar,
        ) -> Result<usize>;
        /// Add a nanosecond duration scalar literal expression to an AST tree.
        ///
        /// # Safety
        ///
        /// The scalar's underlying cuDF allocation must outlive `tree`.
        unsafe fn ast_expression_tree_add_literal_duration_nanoseconds(
            tree: Pin<&mut AstExpressionTree>,
            scalar: &Scalar,
        ) -> Result<usize>;
        /// Add a DECIMAL32 scalar literal expression to an AST tree.
        ///
        /// # Safety
        ///
        /// The scalar's underlying cuDF allocation must outlive `tree`.
        unsafe fn ast_expression_tree_add_literal_decimal32(
            tree: Pin<&mut AstExpressionTree>,
            scalar: &Scalar,
        ) -> Result<usize>;
        /// Add a DECIMAL64 scalar literal expression to an AST tree.
        ///
        /// # Safety
        ///
        /// The scalar's underlying cuDF allocation must outlive `tree`.
        unsafe fn ast_expression_tree_add_literal_decimal64(
            tree: Pin<&mut AstExpressionTree>,
            scalar: &Scalar,
        ) -> Result<usize>;
        /// Add a DECIMAL128 scalar literal expression to an AST tree.
        ///
        /// # Safety
        ///
        /// The scalar's underlying cuDF allocation must outlive `tree`.
        unsafe fn ast_expression_tree_add_literal_decimal128(
            tree: Pin<&mut AstExpressionTree>,
            scalar: &Scalar,
        ) -> Result<usize>;

        /// Add a unary operation expression to an AST tree.
        fn ast_expression_tree_add_unary_operation(
            tree: Pin<&mut AstExpressionTree>,
            ast_operator: i32,
            input_index: usize,
        ) -> Result<usize>;

        /// Add a binary operation expression to an AST tree.
        fn ast_expression_tree_add_operation(
            tree: Pin<&mut AstExpressionTree>,
            ast_operator: i32,
            left_index: usize,
            right_index: usize,
        ) -> Result<usize>;

        /// Opaque owning wrapper for an RMM CUDA stream.
        type CudaStream;

        /// Opaque non-owning wrapper for an RMM CUDA stream view.
        type CudaStreamView;

        /// Opaque non-owning wrapper for an upstream CCCL CUDA stream ref.
        type CudaStreamRef;

        /// Opaque owning wrapper for a CUDA event.
        type CudaEvent;

        /// Return whether this wrapper still owns an underlying CUDA stream.
        ///
        /// In C++, you could do something like
        /// ```ignore
        /// CudaStream a = CudaStream();
        /// CudaStream b(std::move(a));
        /// ```
        /// This would transfer ownership of the stream from `a` to `b`, making `a` invalid.
        ///
        /// However, there's no ffi API available right now to do this via rust, so there's no
        /// need to worry about invalidating a stream at the moment.
        fn is_valid(self: &CudaStream) -> bool;

        /// Synchronize the owned CUDA stream.
        fn synchronize(self: &CudaStream) -> Result<()>;

        /// Return true if this is the CUDA legacy default stream.
        fn is_default(self: &CudaStreamView) -> bool;

        /// Return true if this is the CUDA per-thread default stream.
        fn is_per_thread_default(self: &CudaStreamView) -> bool;

        /// Synchronize the viewed CUDA stream.
        fn synchronize(self: &CudaStreamView) -> Result<()>;

        /// Create and record a CUDA event on this stream.
        ///
        /// `flags` is the raw upstream `cuda::event_flags` bitmask; this keeps
        /// sys mechanical while allowing upstream flag combinations through cxx.
        fn record_event(self: &CudaStreamRef, flags: u32) -> Result<UniquePtr<CudaEvent>>;

        /// Make this stream wait until the CUDA event has completed.
        fn wait_event(self: &CudaStreamRef, event: &CudaEvent) -> Result<()>;

        /// Return whether this wrapper owns a live CUDA event.
        fn is_valid(self: &CudaEvent) -> bool;

        /// Record this CUDA event on the given stream.
        fn record(self: &CudaEvent, stream: &CudaStreamRef) -> Result<()>;

        /// Block until this CUDA event has completed.
        fn sync(self: &CudaEvent) -> Result<()>;

        /// Return true if this CUDA event has completed.
        fn is_done(self: &CudaEvent) -> Result<bool>;

        /// Opaque non-owning wrapper for an RMM device async resource reference.
        type DeviceAsyncResourceRef;

        /// Request for groupby aggregation(s) to perform on a column
        ///
        /// The group membership of each value is determined by the corresponding row
        /// in the original order of keys used to construct the groupby. Contains a column
        /// of values to aggregate and a set of aggregations to perform on those elements.
        type AggregationRequest;

        /// Contiguous storage for groupby aggregation requests.
        type AggregationRequests;

        // GroupBy methods - direct cuDF class methods

        /// Performs grouped aggregations on the specified values
        ///
        /// For each aggregation in a request, `values[i]` is aggregated with all other
        /// `values[j]` where rows `i` and `j` in `keys` are equivalent.
        fn aggregate(
            self: &GroupBy,
            requests: &AggregationRequests,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<GroupByResult>>;

        // AggregationRequest methods

        /// Add an aggregation to this request
        fn add(self: &AggregationRequest, agg: UniquePtr<GroupByAggregation>);

        /// Add an aggregation request to the request span.
        fn add(self: Pin<&mut AggregationRequests>, request: UniquePtr<AggregationRequest>);

        /// Helper to extract columns from a vector by moving them
        type ColumnVectorHelper;

        /// Get the number of columns in the helper
        fn len(self: &ColumnVectorHelper) -> usize;

        /// Check if the helper contains no columns
        fn is_empty(self: &ColumnVectorHelper) -> bool;

        /// Release and take ownership of a column at the specified index
        fn release(self: Pin<&mut ColumnVectorHelper>, index: usize) -> UniquePtr<Column>;

        /// Result pair from a groupby aggregation operation
        ///
        /// Contains both the group keys (unique combinations of key column values) and
        /// the aggregation results for each request. The keys table identifies each group,
        /// and the results contain the computed aggregations for each group.
        type GroupByResult;

        // GroupByResult methods

        /// Take the group labels for each group. Can be done only once and will make all other
        /// keys method fail.
        fn release_keys(self: Pin<&mut GroupByResult>) -> UniquePtr<Table>;

        /// Get the number of aggregation requests
        fn len(self: &GroupByResult) -> usize;

        /// Check if there are no aggregation results
        fn is_empty(self: &GroupByResult) -> bool;

        /// Release and take ownership of the aggregation result at the specified index
        fn release_result(
            self: Pin<&mut GroupByResult>,
            index: usize,
        ) -> UniquePtr<ColumnVectorHelper>;

        // Table methods
        /// Get the number of columns in the table
        fn num_columns(self: &Table) -> Result<i32>;

        /// Get the number of rows in the table
        fn num_rows(self: &Table) -> Result<i32>;

        /// Get a view of this table
        ///
        /// # Safety
        ///
        /// The returned view must not outlive this table.
        unsafe fn view(self: &Table) -> Result<UniquePtr<TableView>>;

        /// Get a const column reference at index.
        ///
        /// # Safety
        ///
        /// The returned reference wrapper must not outlive this table.
        unsafe fn get_column(self: &Table, index: i32) -> Result<UniquePtr<ColumnRef>>;

        /// Release and take ownership of the table's columns
        fn release(self: Pin<&mut Table>) -> Result<UniquePtr<ColumnVectorHelper>>;

        // TableView methods
        /// Get the number of columns in the table view
        fn num_columns(self: &TableView) -> Result<i32>;

        /// Get the number of rows in the table view
        fn num_rows(self: &TableView) -> Result<i32>;

        /// Select specific columns by indices
        fn select(self: &TableView, column_indices: &[i32]) -> Result<UniquePtr<TableView>>;

        /// Get column view at index
        fn column(self: &TableView, index: i32) -> Result<UniquePtr<ColumnView>>;

        /// Mechanical factory for `cudf::table_view`'s copy constructor.
        fn table_view_clone(view: &TableView) -> Result<UniquePtr<TableView>>;

        // Column methods
        /// Get the number of elements in the column
        fn size(self: &Column) -> Result<i32>;

        /// Get the data type of the column
        #[cxx_name = "type"]
        fn data_type(self: &Column) -> Result<UniquePtr<DataType>>;

        /// Get the total device allocation size of the column.
        fn alloc_size(self: &Column) -> Result<usize>;

        /// Get the total device allocation size of a referenced column.
        fn alloc_size(self: &ColumnRef) -> Result<usize>;

        // ColumnView methods
        /// Get the number of elements in the column view
        fn size(self: &ColumnView) -> Result<i32>;

        /// Get a view of this column
        ///
        /// # Safety
        ///
        /// The returned view must not outlive this column.
        unsafe fn view(self: &Column) -> Result<UniquePtr<ColumnView>>;

        /// Get the raw device pointer to the column view's data
        fn head(self: &ColumnView) -> Result<usize>;

        /// Get the data type of the column view
        #[cxx_name = "type"]
        fn data_type(self: &ColumnView) -> Result<UniquePtr<DataType>>;

        /// Mechanical factory for `cudf::column_view`'s copy constructor.
        fn column_view_clone(view: &ColumnView) -> Result<UniquePtr<ColumnView>>;

        /// Get the offset of the current ColumnView in case it was a slice of another one
        fn offset(self: &ColumnView) -> Result<i32>;

        /// Get the number of null values in the column
        fn null_count(self: &ColumnView) -> Result<i32>;

        /// Return whether the column view has a null mask.
        fn nullable(self: &ColumnView) -> Result<bool>;

        /// Return the number of child column views.
        fn num_children(self: &ColumnView) -> Result<i32>;

        /// Return a child column view by index.
        ///
        /// The index must be less than `num_children`, matching cuDF's
        /// unchecked `column_view::child` contract.
        unsafe fn child(self: &ColumnView, index: i32) -> Result<UniquePtr<ColumnView>>;

        /// Return `column_view::null_mask()` encoded as an integer because cxx
        /// cannot return `bitmask_type const*` directly.
        fn null_mask(self: &ColumnView) -> Result<usize>;

        /// Construct the full pointer-based `cudf::column_view` overload.
        unsafe fn column_view_create(
            dtype: &DataType,
            size: i32,
            data: usize,
            null_mask: usize,
            null_count: i32,
            offset: i32,
            children: &[*const ColumnView],
        ) -> Result<UniquePtr<ColumnView>>;

        // DataType methods
        /// Get the type_id
        fn id(self: &DataType) -> i32;

        /// Get the scale (for fixed_point types)
        fn scale(self: &DataType) -> i32;

        /// Return whether `type` has a fixed-width representation.
        fn is_fixed_width(dtype: &DataType) -> bool;

        /// Return the byte width of a fixed-width `type`.
        fn size_of(dtype: &DataType) -> Result<usize>;

        /// Return the padded allocation size for a null mask.
        fn bitmask_allocation_size_bytes(number_of_bits: i32, padding_boundary: usize) -> usize;

        /// Direct wrapper for `cudf::get_column_metadata`.
        fn get_column_metadata(input: &ColumnView) -> Result<UniquePtr<ColumnMetadata>>;

        /// Direct wrapper for `cudf::get_table_metadata`.
        fn get_table_metadata(input: &TableView) -> Result<UniquePtr<ColumnMetadataVector>>;

        /// Direct wrapper for `cudf::to_arrow_schema`.
        unsafe fn to_arrow_schema(
            input: &TableView,
            metadata: &ColumnMetadataVector,
            out_schema_ptr: *mut u8,
        ) -> Result<()>;

        /// Direct wrapper for the table-view overload of `cudf::to_arrow_host`.
        #[rust_name = "to_arrow_host_table"]
        unsafe fn to_arrow_host(
            input: &TableView,
            out_array_ptr: *mut u8,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<()>;

        /// Direct wrapper for the column-view overload of `cudf::to_arrow_host`.
        #[rust_name = "to_arrow_host_column"]
        unsafe fn to_arrow_host(
            input: &ColumnView,
            out_array_ptr: *mut u8,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<()>;

        // Scalar methods
        /// Check if the scalar is valid (not null)
        fn is_valid(self: &Scalar, stream: &CudaStreamView) -> Result<bool>;

        /// Get the data type of the scalar
        #[cxx_name = "type"]
        fn data_type(self: &Scalar) -> Result<UniquePtr<DataType>>;

        // Factory functions

        /// Create a DataType from a type_id
        fn new_data_type(type_id: i32) -> UniquePtr<DataType>;

        /// Create a DataType from a type_id and scale (for decimals)
        fn new_data_type_with_scale(type_id: i32, scale: i32) -> UniquePtr<DataType>;

        /// Create an empty table with no columns and no rows
        fn create_empty_table() -> Result<UniquePtr<Table>>;

        /// Create a table from a set of column pointers (takes ownership)
        /// The columns are consumed and should not be used after this call
        unsafe fn create_table_from_columns_move(
            columns: &[*mut Column],
        ) -> Result<UniquePtr<Table>>;

        /// Concatenate table views vertically.
        #[rust_name = "concatenate_tables"]
        fn concatenate(
            views: &[UniquePtr<TableView>],
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<Table>>;

        /// Concatenate column views vertically.
        #[rust_name = "concatenate_columns"]
        fn concatenate(
            views: &[UniquePtr<ColumnView>],
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<Column>>;

        /// Create a column by repeating a scalar value.
        fn make_column_from_scalar(
            scalar: &Scalar,
            size: i32,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<Column>>;

        /// Fill a column with a sequence starting at `init` and stepping by one.
        #[rust_name = "sequence_from"]
        fn sequence(
            size: i32,
            init: &Scalar,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<Column>>;

        /// Fill a column with a sequence starting at `init` and stepping by `step`.
        #[rust_name = "sequence_with_step"]
        fn sequence(
            size: i32,
            init: &Scalar,
            step: &Scalar,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<Column>>;

        /// Create a TableView from a set of ColumnView pointers (non-owning)
        unsafe fn create_table_view_from_column_views(
            column_views: &[*const ColumnView],
        ) -> Result<UniquePtr<TableView>>;

        // Parquet I/O

        /// Construct `cudf::io::source_info` from one file path.
        fn source_info_from_file_path(file_path: &str) -> UniquePtr<SourceInfo>;

        /// Construct `cudf::io::source_info` from multiple file paths.
        fn source_info_from_file_paths(file_paths: Vec<String>) -> UniquePtr<SourceInfo>;

        /// Return the number of sources in this `source_info`.
        fn num_sources(self: &SourceInfo) -> usize;

        /// Construct `cudf::io::sink_info` from one file path.
        fn sink_info_from_file_path(file_path: &str) -> UniquePtr<SinkInfo>;

        /// Construct `cudf::io::sink_info` from multiple file paths.
        fn sink_info_from_file_paths(file_paths: Vec<String>) -> UniquePtr<SinkInfo>;

        /// Return the number of sinks in this `sink_info`.
        fn num_sinks(self: &SinkInfo) -> usize;

        /// Build `cudf::io::parquet_reader_options` from `source_info`.
        fn parquet_reader_options_create(source: &SourceInfo) -> UniquePtr<ParquetReaderOptions>;

        /// Build `cudf::io::parquet_writer_options` from `sink_info` and a table view.
        fn parquet_writer_options_create(
            sink: &SinkInfo,
            table: &TableView,
        ) -> Result<UniquePtr<ParquetWriterOptions>>;

        /// Set the source on `cudf::io::parquet_reader_options`.
        fn set_source(self: Pin<&mut ParquetReaderOptions>, source: &SourceInfo);

        /// Set projected columns on `cudf::io::parquet_reader_options`.
        fn set_columns(self: Pin<&mut ParquetReaderOptions>, col_names: Vec<String>);

        /// Set per-source row groups on `cudf::io::parquet_reader_options`.
        ///
        /// This is a flat FFI representation of cuDF's nested row group vector.
        fn set_row_groups(
            self: Pin<&mut ParquetReaderOptions>,
            row_group_indices: Vec<i32>,
            source_offsets: Vec<usize>,
        );

        /// Set AST filter on `cudf::io::parquet_reader_options`.
        fn set_filter(
            self: Pin<&mut ParquetReaderOptions>,
            filter: &AstExpressionTree,
        ) -> Result<()>;

        /// Enable conversion of strings to categories.
        fn enable_convert_strings_to_categories(self: Pin<&mut ParquetReaderOptions>, val: bool);

        /// Enable reading pandas metadata.
        fn enable_use_pandas_metadata(self: Pin<&mut ParquetReaderOptions>, val: bool);

        /// Enable reading Arrow schema metadata.
        fn enable_use_arrow_schema(self: Pin<&mut ParquetReaderOptions>, val: bool);

        /// Enable reading matching projected and filter columns from mismatched Parquet sources.
        fn enable_allow_mismatched_pq_schemas(self: Pin<&mut ParquetReaderOptions>, val: bool);

        /// Enable ignoring non-existent projected columns while reading.
        fn enable_ignore_missing_columns(self: Pin<&mut ParquetReaderOptions>, val: bool);

        /// Set number of rows to skip.
        fn set_skip_rows(self: Pin<&mut ParquetReaderOptions>, val: i64);

        /// Set number of rows to read.
        fn set_num_rows(self: Pin<&mut ParquetReaderOptions>, val: i64);

        /// Set bytes to skip before starting row group reads.
        fn set_skip_bytes(self: Pin<&mut ParquetReaderOptions>, val: usize);

        /// Set number of bytes after skipping to end row group reads at.
        fn set_num_bytes(self: Pin<&mut ParquetReaderOptions>, val: usize);

        /// Set timestamp type used to cast timestamp columns.
        fn set_timestamp_type(self: Pin<&mut ParquetReaderOptions>, typ: &DataType);

        /// Take ownership of the table from `cudf::io::table_with_metadata`.
        fn release_table(self: Pin<&mut TableWithMetadata>) -> Result<UniquePtr<Table>>;

        /// Take ownership of all metadata from `cudf::io::table_with_metadata`.
        fn release_metadata(self: Pin<&mut TableWithMetadata>) -> Result<UniquePtr<TableMetadata>>;

        /// Return the number of schema entries.
        fn schema_info_len(self: &TableMetadata) -> usize;

        /// Return one recursive schema entry by value.
        fn schema_info(self: &TableMetadata, index: usize) -> Result<UniquePtr<ColumnNameInfo>>;

        /// Return the number of per-source row counts.
        fn num_rows_per_source_len(self: &TableMetadata) -> usize;

        /// Return one per-source row count.
        fn num_rows_per_source(self: &TableMetadata, index: usize) -> Result<usize>;

        /// Return the first input file's user metadata.
        fn user_data(self: &TableMetadata) -> UniquePtr<KeyValuePairs>;

        /// Return the number of per-file user metadata maps.
        fn per_file_user_data_len(self: &TableMetadata) -> usize;

        /// Return one input file's user metadata.
        fn per_file_user_data(
            self: &TableMetadata,
            index: usize,
        ) -> Result<UniquePtr<KeyValuePairs>>;

        /// Return the total number of input row groups.
        fn num_input_row_groups(self: &TableMetadata) -> i32;

        /// Return whether the post-statistics-filter row-group count is present.
        fn has_num_row_groups_after_stats_filter(self: &TableMetadata) -> bool;

        /// Return the post-statistics-filter row-group count.
        fn num_row_groups_after_stats_filter(self: &TableMetadata) -> Result<i32>;

        /// Return whether the post-bloom-filter row-group count is present.
        fn has_num_row_groups_after_bloom_filter(self: &TableMetadata) -> bool;

        /// Return the post-bloom-filter row-group count.
        fn num_row_groups_after_bloom_filter(self: &TableMetadata) -> Result<i32>;

        /// Return the column name.
        fn name(self: &ColumnNameInfo) -> String;

        /// Return whether nullability metadata is present.
        fn has_is_nullable(self: &ColumnNameInfo) -> bool;

        /// Return the column nullability metadata.
        fn is_nullable(self: &ColumnNameInfo) -> Result<bool>;

        /// Return whether binary-encoding metadata is present.
        fn has_is_binary(self: &ColumnNameInfo) -> bool;

        /// Return the binary-encoding metadata.
        fn is_binary(self: &ColumnNameInfo) -> Result<bool>;

        /// Return whether fixed-width byte-length metadata is present.
        fn has_type_length(self: &ColumnNameInfo) -> bool;

        /// Return the fixed-width byte length.
        fn type_length(self: &ColumnNameInfo) -> Result<i32>;

        /// Return the number of recursive child schema entries.
        fn children_len(self: &ColumnNameInfo) -> usize;

        /// Return one recursive child schema entry by value.
        fn child(self: &ColumnNameInfo, index: usize) -> Result<UniquePtr<ColumnNameInfo>>;

        /// Return the number of metadata entries.
        fn len(self: &KeyValuePairs) -> usize;

        /// Return whether there are no metadata entries.
        fn is_empty(self: &KeyValuePairs) -> bool;

        /// Return one metadata key.
        fn key(self: &KeyValuePairs, index: usize) -> Result<String>;

        /// Return one metadata value.
        fn value(self: &KeyValuePairs, index: usize) -> Result<String>;

        /// Read Parquet using explicit reader options, CUDA stream, and device resource.
        fn read_parquet(
            options: &ParquetReaderOptions,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<TableWithMetadata>>;

        /// Create a cuDF chunked Parquet reader.
        fn chunked_parquet_reader_create(
            chunk_read_limit: usize,
            pass_read_limit: usize,
            options: &ParquetReaderOptions,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<ChunkedParquetReader>>;

        /// Return whether a chunked Parquet reader has unread rows.
        fn has_next(self: &ChunkedParquetReader) -> Result<bool>;

        /// Read the next chunk from a chunked Parquet reader.
        fn read_chunk(self: &ChunkedParquetReader) -> Result<UniquePtr<TableWithMetadata>>;

        /// Return the size of the returned host byte vector.
        fn size(self: &HostByteVector) -> Result<usize>;

        /// Write Parquet using explicit writer options and CUDA stream.
        fn write_parquet(
            options: &ParquetWriterOptions,
            stream: &CudaStreamView,
        ) -> Result<UniquePtr<HostByteVector>>;

        // Direct cuDF operations

        /// Filters a table using a boolean mask
        ///
        /// Given an input table and a mask column, an element `i` from each column of the input
        /// is copied to the corresponding output column if the corresponding element `i` in the
        /// mask is non-null and `true`. This operation is stable: the input order is preserved.
        fn apply_boolean_mask(
            table: &TableView,
            boolean_mask: &ColumnView,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<Table>>;

        /// Gather rows from a table based on a gather map (column of indices)
        ///
        /// Reorders the rows of `source_table` according to the indices in `gather_map`.
        /// The resulting table will have the same number of rows as `gather_map` has elements.
        fn gather(
            source_table: &TableView,
            gather_map: &ColumnView,
            out_of_bounds_policy: i32,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<Table>>;

        /// Scatter table rows into a copy of a target table.
        #[rust_name = "scatter_table"]
        fn scatter(
            source: &TableView,
            indices: &ColumnView,
            target: &TableView,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<Table>>;

        /// Scatter scalar rows into a copy of a target table.
        #[rust_name = "scatter_scalars"]
        fn scatter(
            source: &[*const Scalar],
            indices: &ColumnView,
            target: &TableView,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<Table>>;

        /// Create a table without duplicate rows.
        fn distinct(
            input: &TableView,
            keys: &[i32],
            keep: i32,
            nulls_equal: i32,
            nans_equal: i32,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<Table>>;

        /// Slice a column view at each `[begin, end)` pair in `indices`.
        ///
        /// # Safety
        ///
        /// Every returned view borrows from `column`'s underlying data owner.
        #[rust_name = "slice_column"]
        unsafe fn slice(
            column: &ColumnView,
            indices: &[i32],
            stream: &CudaStreamView,
        ) -> Result<UniquePtr<ColumnViews>>;

        /// Slice a table view at each `[begin, end)` pair in `indices`.
        ///
        /// # Safety
        ///
        /// Every returned view borrows from `table`'s underlying data owners.
        #[rust_name = "slice_table"]
        unsafe fn slice(
            table: &TableView,
            indices: &[i32],
            stream: &CudaStreamView,
        ) -> Result<UniquePtr<TableViews>>;

        // Binary operations - direct cuDF mappings

        /// Perform a binary operation between two columns
        ///
        /// Returns a new column containing the result of `op(lhs[i], rhs[i])` for all elements.
        /// The output type must be specified explicitly.
        #[rust_name = "binary_operation_column_column"]
        fn binary_operation(
            lhs: &ColumnView,
            rhs: &ColumnView,
            op: i32,
            output_type: &DataType,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<Column>>;

        /// Perform a binary operation between a column and a scalar
        ///
        /// Returns a new column containing the result of `op(lhs[i], rhs)` for all elements.
        /// The output type must be specified explicitly.
        #[rust_name = "binary_operation_column_scalar"]
        fn binary_operation(
            lhs: &ColumnView,
            rhs: &Scalar,
            op: i32,
            output_type: &DataType,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<Column>>;

        /// Perform a binary operation between a scalar and a column
        ///
        /// Returns a new column containing the result of `op(lhs, rhs[i])` for all elements.
        /// The output type must be specified explicitly.
        #[rust_name = "binary_operation_scalar_column"]
        fn binary_operation(
            lhs: &Scalar,
            rhs: &ColumnView,
            op: i32,
            output_type: &DataType,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<Column>>;

        /// Perform a PTX-defined binary operation between two columns.
        #[rust_name = "binary_operation_ptx"]
        fn binary_operation(
            lhs: &ColumnView,
            rhs: &ColumnView,
            ptx: &str,
            output_type: &DataType,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<Column>>;

        // Sorting operations - direct cuDF mappings

        /// Sort a table in lexicographic order
        ///
        /// Sorts the rows of the table according to the specified column orders and null precedence.
        fn sort(
            input: &TableView,
            column_order: &[i32],
            null_precedence: &[i32],
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<Table>>;

        /// Stable sort a table in lexicographic order
        ///
        /// Like `sort` but guarantees that equivalent elements preserve their original order.
        fn stable_sort(
            input: &TableView,
            column_order: &[i32],
            null_precedence: &[i32],
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<Table>>;

        /// Get the indices that would sort a table
        ///
        /// Returns a column of indices that would produce a sorted table if used to reorder the rows.
        fn sorted_order(
            input: &TableView,
            column_order: &[i32],
            null_precedence: &[i32],
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<Column>>;

        /// Get the indices that would stably sort a table
        ///
        /// Like sorted_order but preserves the relative order of equivalent elements.
        fn stable_sorted_order(
            input: &TableView,
            column_order: &[i32],
            null_precedence: &[i32],
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<Column>>;

        /// Check if a table is sorted
        ///
        /// Returns true if the rows are sorted according to the specified column orders.
        fn is_sorted(
            input: &TableView,
            column_order: &[i32],
            null_precedence: &[i32],
            stream: &CudaStreamView,
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
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<Table>>;

        /// Stable sort values table based on keys table
        ///
        /// Same as `sort_by_key` but preserves the relative order of equivalent elements.
        fn stable_sort_by_key(
            values: &TableView,
            keys: &TableView,
            column_order: &[i32],
            null_precedence: &[i32],
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<Table>>;

        // Join operations.

        /// Sentinel row index cuDF uses for unmatched join rows.
        fn join_no_match() -> i32;

        /// Create a reusable hash join object from build-side keys.
        fn hash_join_create(
            build_keys: &TableView,
            null_equality: i32,
            stream: &CudaStreamView,
        ) -> Result<UniquePtr<HashJoin>>;

        /// Probe a reusable hash join object and return probe/build row indices.
        fn hash_join_inner_join_indices(
            join: &HashJoin,
            probe_keys: &TableView,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<HashJoinIndices>>;

        /// Probe a reusable hash join object preserving probe rows.
        fn hash_join_left_join_indices(
            join: &HashJoin,
            probe_keys: &TableView,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<HashJoinIndices>>;

        /// Inner join: return row index maps for the matching rows.
        fn inner_join_indices(
            left_keys: &TableView,
            right_keys: &TableView,
            null_equality: i32,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<JoinIndices>>;

        /// Left join: return row index maps for the output rows.
        fn left_join_indices(
            left_keys: &TableView,
            right_keys: &TableView,
            null_equality: i32,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<JoinIndices>>;

        /// Full join: return row index maps for the output rows.
        fn full_join_indices(
            left_keys: &TableView,
            right_keys: &TableView,
            null_equality: i32,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<JoinIndices>>;

        /// Filter join index maps with a cuDF AST predicate.
        #[allow(clippy::too_many_arguments)]
        fn filter_join_indices(
            left: &TableView,
            right: &TableView,
            left_indices: &DeviceIndexVector,
            right_indices: &DeviceIndexVector,
            predicate: &AstExpressionTree,
            join_kind: i32,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<JoinIndices>>;

        /// Create a reusable filtered join object from build-side keys.
        fn filtered_join_create(
            build_keys: &TableView,
            null_equality: i32,
            set_as_build_table: i32,
            stream: &CudaStreamView,
        ) -> Result<UniquePtr<FilteredJoin>>;

        /// Return row indices for a filtered semi join.
        fn filtered_join_semi_join(
            join: &FilteredJoin,
            probe_keys: &TableView,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<DeviceIndexVector>>;

        /// Return row indices for a filtered anti join.
        fn filtered_join_anti_join(
            join: &FilteredJoin,
            probe_keys: &TableView,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<DeviceIndexVector>>;

        /// Cross join: returns a full Cartesian product table
        fn cross_join(
            left: &TableView,
            right: &TableView,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<Table>>;

        // Aggregation factory functions - direct cuDF mappings (for reduce)

        /// Create a SUM aggregation
        fn make_sum_aggregation() -> UniquePtr<ReduceAggregation>;

        /// Create a MIN aggregation
        fn make_min_aggregation() -> UniquePtr<ReduceAggregation>;

        /// Create a MAX aggregation
        fn make_max_aggregation() -> UniquePtr<ReduceAggregation>;

        /// Create a MEAN aggregation
        fn make_mean_aggregation() -> UniquePtr<ReduceAggregation>;

        /// Create a COUNT aggregation
        fn make_count_aggregation(null_handling: i32) -> UniquePtr<ReduceAggregation>;

        /// Create a VARIANCE aggregation
        fn make_variance_aggregation(ddof: i32) -> UniquePtr<ReduceAggregation>;

        /// Create a STD aggregation
        fn make_std_aggregation(ddof: i32) -> UniquePtr<ReduceAggregation>;

        /// Create a NUNIQUE aggregation
        fn make_nunique_aggregation(null_handling: i32) -> UniquePtr<ReduceAggregation>;

        /// Create a MEDIAN aggregation
        fn make_median_aggregation() -> UniquePtr<ReduceAggregation>;

        // Aggregation factory functions - direct cuDF mappings (for groupby)

        /// Create a SUM aggregation for groupby operations
        fn make_sum_aggregation_groupby() -> UniquePtr<GroupByAggregation>;

        /// Create a MIN aggregation for groupby operations
        fn make_min_aggregation_groupby() -> UniquePtr<GroupByAggregation>;

        /// Create a MAX aggregation for groupby operations
        fn make_max_aggregation_groupby() -> UniquePtr<GroupByAggregation>;

        /// Create a MEAN aggregation for groupby operations
        fn make_mean_aggregation_groupby() -> UniquePtr<GroupByAggregation>;

        /// Create a COUNT aggregation for groupby operations
        fn make_count_aggregation_groupby(null_handling: i32) -> UniquePtr<GroupByAggregation>;

        /// Create a VARIANCE aggregation for groupby operations
        fn make_variance_aggregation_groupby(ddof: i32) -> UniquePtr<GroupByAggregation>;

        /// Create a STD aggregation for groupby operations
        fn make_std_aggregation_groupby(ddof: i32) -> UniquePtr<GroupByAggregation>;

        /// Create a NUNIQUE aggregation for groupby operations
        fn make_nunique_aggregation_groupby(null_handling: i32) -> UniquePtr<GroupByAggregation>;

        /// Create a MEDIAN aggregation for groupby operations
        fn make_median_aggregation_groupby() -> UniquePtr<GroupByAggregation>;

        // Reduction - direct cuDF mapping

        /// Computes the reduction of the values in all rows of a column
        ///
        /// This function does not detect overflows in reductions. Any null values are skipped
        /// for the operation. If the reduction fails, the output scalar returns with `is_valid()==false`.
        fn reduce(
            col: &ColumnView,
            agg: &ReduceAggregation,
            output_type: &DataType,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<Scalar>>;

        /// Computes a reduction with an initial scalar value.
        #[rust_name = "reduce_with_init"]
        fn reduce(
            col: &ColumnView,
            agg: &ReduceAggregation,
            output_type: &DataType,
            init: &Scalar,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<Scalar>>;

        // GroupBy operations - direct cuDF mappings

        /// Construct a groupby object with the specified keys
        ///
        /// The groupby object groups values by keys and computes aggregations on those groups.
        fn groupby_create(
            keys: &TableView,
            null_handling: i32,
            keys_are_sorted: i32,
            column_order: &[i32],
            null_precedence: &[i32],
        ) -> UniquePtr<GroupBy>;

        /// Create an aggregation request for a column of values
        ///
        /// The group membership of each `value[i]` is determined by the corresponding row `i`
        /// in the original order of `keys` used to construct the groupby.
        fn aggregation_request_create(values: &ColumnView) -> UniquePtr<AggregationRequest>;

        /// Create contiguous storage for groupby aggregation requests.
        fn aggregation_requests_create() -> UniquePtr<AggregationRequests>;

        // Arrow interop - direct cuDF calls

        /// Convert an Arrow DeviceArray to a cuDF table
        ///
        /// # Safety
        ///
        /// Pointers must be valid Arrow C Data Interface structures.
        unsafe fn from_arrow_host(
            schema_ptr: *const u8,
            device_array_ptr: *const u8,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<Table>>;

        /// Convert an Arrow array to a cuDF column
        ///
        /// # Safety
        ///
        /// Pointers must be valid Arrow C Data Interface structures.
        unsafe fn from_arrow_column(
            schema_ptr: *const u8,
            array_ptr: *const u8,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<Column>>;

        /// Cast a column to a different data type using GPU-native cudf::cast
        fn cast(
            input: &ColumnView,
            target_type: &DataType,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<Column>>;

        /// Extract a scalar from a column at the specified index
        fn get_element(
            column: &ColumnView,
            index: i32,
            stream: &CudaStreamView,
            mr: &DeviceAsyncResourceRef,
        ) -> Result<UniquePtr<Scalar>>;

        /// Get the version of the cuDF library
        fn get_cudf_version() -> String;

        /// Opaque wrapper around `cudf::pinned_mr_options`.
        type PinnedMrOptions;

        /// Construct options with upstream's default `pool_size = nullopt`.
        fn make_pinned_mr_options() -> UniquePtr<PinnedMrOptions>;

        /// Construct options with an explicit pool size.
        fn make_pinned_mr_options_with_pool_size(
            pool_size_bytes: usize,
        ) -> UniquePtr<PinnedMrOptions>;

        /// Direct wrapper for `cudf::config_default_pinned_memory_resource`.
        fn config_default_pinned_memory_resource(options: &PinnedMrOptions) -> bool;

        /// Set cuDF's host allocation threshold for pinned memory.
        fn set_allocate_host_as_pinned_threshold(threshold_bytes: usize);

        /// Create a CUDA stream using the default creation flag.
        fn cuda_stream_create() -> Result<UniquePtr<CudaStream>>;

        /// Create a CUDA stream with explicit creation flags.
        fn cuda_stream_create_with_flags(flags: u32) -> Result<UniquePtr<CudaStream>>;

        /// Return a non-owning view for an owned CUDA stream.
        ///
        /// # Safety
        ///
        /// The returned view must not outlive `stream`.
        unsafe fn cuda_stream_view(stream: &CudaStream) -> UniquePtr<CudaStreamView>;

        /// Convert an RMM CUDA stream view into an upstream CCCL CUDA stream ref.
        ///
        /// Factory function because cxx cannot expose C++ conversion operators directly.
        fn cuda_stream_ref_from_view(stream: &CudaStreamView) -> UniquePtr<CudaStreamRef>;

        /// Create a CUDA event on a device with explicit upstream event flags.
        ///
        /// Factory function because cxx cannot expose C++ constructors directly.
        fn cuda_event_create_on_device(device_id: i32, flags: u32) -> Result<UniquePtr<CudaEvent>>;

        /// Get cuDF's current default stream.
        fn get_default_stream() -> UniquePtr<CudaStreamView>;

        /// Check whether cuDF is using the CUDA per-thread default stream.
        fn is_ptds_enabled() -> bool;

        /// Return the current CUDA device ordinal.
        fn cuda_get_device() -> Result<i32>;

        /// Set the current CUDA device ordinal.
        fn cuda_set_device(device_id: i32) -> Result<()>;

        /// Direct wrapper for `cudaMemcpy`. Pointer values are integer-encoded
        /// because cxx cannot expose untyped CUDA pointers directly.
        unsafe fn cuda_memcpy(destination: usize, source: usize, count: usize, kind: i32) -> i32;

        /// Direct wrapper for `cudaGetErrorString`.
        fn cuda_get_error_string(error: i32) -> String;

        /// Return cuDF's current device memory resource reference.
        fn get_current_device_resource_ref() -> UniquePtr<DeviceAsyncResourceRef>;

        /// Set cuDF's current device memory resource reference.
        ///
        /// # Safety
        ///
        /// The referenced resource must remain alive until it is replaced or
        /// reset and all work using it has completed.
        unsafe fn set_current_device_resource_ref(
            resource: &DeviceAsyncResourceRef,
        ) -> UniquePtr<DeviceAsyncResourceRef>;

        /// Reset cuDF's current device memory resource reference to the initial resource.
        fn reset_current_device_resource_ref() -> UniquePtr<DeviceAsyncResourceRef>;

        /// Compare two device async resource references.
        fn device_async_resource_ref_equal(
            lhs: &DeviceAsyncResourceRef,
            rhs: &DeviceAsyncResourceRef,
        ) -> bool;

        /// Opaque wrapper around `rmm::host_device_async_resource_ref`.
        type HostDeviceAsyncResourceRef;

        /// Allocate pinned host memory through the referenced resource.
        /// The returned pointer is encoded as `usize` because cxx does not
        /// currently expose `*mut u8` return values across the bridge.
        fn allocate_sync(self: &HostDeviceAsyncResourceRef, bytes: usize) -> Result<usize>;

        /// Deallocate pinned host memory through the referenced resource.
        fn deallocate_sync(self: &HostDeviceAsyncResourceRef, ptr: usize, bytes: usize);

        /// Return cuDF's process-global pinned memory resource handle.
        fn get_pinned_memory_resource() -> UniquePtr<HostDeviceAsyncResourceRef>;

        /// Opaque owning wrapper around `rmm::mr::cuda_memory_resource`.
        type CudaMemoryResource;

        /// Construct an RMM CUDA memory resource.
        fn make_cuda_memory_resource() -> UniquePtr<CudaMemoryResource>;

        /// Opaque owning wrapper around an RMM pool memory resource.
        type PoolMemoryResource;

        /// Construct an RMM pool memory resource.
        ///
        /// # Safety
        ///
        /// `upstream` must outlive the returned pool.
        unsafe fn make_pool_memory_resource(
            upstream: &CudaMemoryResource,
            initial_size: usize,
        ) -> Result<UniquePtr<PoolMemoryResource>>;

        /// Construct an RMM pool memory resource with an explicit maximum size.
        ///
        /// # Safety
        ///
        /// `upstream` must outlive the returned pool.
        unsafe fn make_pool_memory_resource_with_maximum(
            upstream: &CudaMemoryResource,
            initial_size: usize,
            maximum_size: usize,
        ) -> Result<UniquePtr<PoolMemoryResource>>;

        /// Construct RMM's non-owning resource-reference type from a pool.
        ///
        /// # Safety
        ///
        /// `resource` must outlive the returned reference and every operation
        /// to which that reference is passed.
        unsafe fn make_device_async_resource_ref(
            resource: &PoolMemoryResource,
        ) -> UniquePtr<DeviceAsyncResourceRef>;

        /// Opaque wrapper for the pair returned by `rmm::available_device_memory`.
        type AvailableDeviceMemory;

        /// Return the pair's available-memory component.
        fn free_bytes(self: &AvailableDeviceMemory) -> usize;

        /// Return the pair's total-memory component.
        fn total_bytes(self: &AvailableDeviceMemory) -> usize;

        /// Direct wrapper for `rmm::available_device_memory`.
        fn available_device_memory() -> Result<UniquePtr<AvailableDeviceMemory>>;
    }
}

/// `cudaMemcpyHostToHost` from `cudaMemcpyKind`.
pub const CUDA_MEMCPY_HOST_TO_HOST: i32 = 0;
/// `cudaMemcpyHostToDevice` from `cudaMemcpyKind`.
pub const CUDA_MEMCPY_HOST_TO_DEVICE: i32 = 1;
/// `cudaMemcpyDeviceToHost` from `cudaMemcpyKind`.
pub const CUDA_MEMCPY_DEVICE_TO_HOST: i32 = 2;
/// `cudaMemcpyDeviceToDevice` from `cudaMemcpyKind`.
pub const CUDA_MEMCPY_DEVICE_TO_DEVICE: i32 = 3;
/// `cudaMemcpyDefault` from `cudaMemcpyKind`.
pub const CUDA_MEMCPY_DEFAULT: i32 = 4;
/// `cudaSuccess` from `cudaError_t`.
pub const CUDA_SUCCESS: i32 = 0;

/// Return cuDF's current default stream as an explicit stream-view handle.
pub fn get_default_stream() -> cxx::UniquePtr<ffi::CudaStreamView> {
    ffi::get_default_stream()
}

/// Return cuDF's current device memory resource reference.
pub fn get_current_device_resource_ref() -> cxx::UniquePtr<ffi::DeviceAsyncResourceRef> {
    ffi::get_current_device_resource_ref()
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

/// Null handling policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum NullPolicy {
    /// Exclude null elements.
    Exclude = 0,
    /// Include null elements.
    Include = 1,
}

/// Whether values are known to be sorted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum Sorted {
    /// Values are not known to be sorted.
    No = 0,
    /// Values are known to be sorted.
    Yes = 1,
}

/// Policy for out-of-bounds gather indices.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum OutOfBoundsPolicy {
    /// Out-of-bounds rows become null rows.
    Nullify = 0,
    /// Do not check bounds.
    DontCheck = 1,
}

/// Null comparison policy for join keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum NullEquality {
    /// Null values compare equal.
    Equal = 0,
    /// Null values do not compare equal.
    Unequal = 1,
}

/// cuDF join kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum JoinKind {
    /// Inner join.
    Inner = 0,
    /// Left join.
    Left = 1,
    /// Full join.
    Full = 2,
    /// Left semi join.
    LeftSemi = 3,
    /// Left anti join.
    LeftAnti = 4,
}

/// Which table a reusable filtered join treats as its build table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum SetAsBuildTable {
    /// The build table is the left table.
    Left = 0,
    /// The build table is the right table.
    Right = 1,
}

/// cuDF AST table reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum AstTableReference {
    /// Column index in the left table.
    Left = 0,
    /// Column index in the right table.
    Right = 1,
    /// Column index in the output table.
    Output = 2,
}

/// cuDF AST operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum AstOperator {
    /// Addition.
    Add = 0,
    /// Subtraction.
    Sub = 1,
    /// Multiplication.
    Mul = 2,
    /// Division.
    Div = 3,
    /// True division.
    TrueDiv = 4,
    /// Floor division.
    FloorDiv = 5,
    /// Modulo.
    Mod = 6,
    /// Python-style modulo.
    PyMod = 7,
    /// Power.
    Pow = 8,
    /// Equality comparison.
    Equal = 9,
    /// Null-aware equality comparison.
    NullEqual = 10,
    /// Non-equality comparison.
    NotEqual = 11,
    /// Less-than comparison.
    Less = 12,
    /// Greater-than comparison.
    Greater = 13,
    /// Less-than-or-equal comparison.
    LessEqual = 14,
    /// Greater-than-or-equal comparison.
    GreaterEqual = 15,
    /// Bitwise AND.
    BitwiseAnd = 16,
    /// Bitwise OR.
    BitwiseOr = 17,
    /// Bitwise XOR.
    BitwiseXor = 18,
    /// Logical AND.
    LogicalAnd = 19,
    /// Null-aware logical AND.
    NullLogicalAnd = 20,
    /// Logical OR.
    LogicalOr = 21,
    /// Null-aware logical OR.
    NullLogicalOr = 22,
    /// Identity.
    Identity = 23,
    /// Null check.
    IsNull = 24,
    /// Sine.
    Sin = 25,
    /// Cosine.
    Cos = 26,
    /// Tangent.
    Tan = 27,
    /// Inverse sine.
    ArcSin = 28,
    /// Inverse cosine.
    ArcCos = 29,
    /// Inverse tangent.
    ArcTan = 30,
    /// Hyperbolic sine.
    Sinh = 31,
    /// Hyperbolic cosine.
    Cosh = 32,
    /// Hyperbolic tangent.
    Tanh = 33,
    /// Inverse hyperbolic sine.
    ArcSinh = 34,
    /// Inverse hyperbolic cosine.
    ArcCosh = 35,
    /// Inverse hyperbolic tangent.
    ArcTanh = 36,
    /// Exponential.
    Exp = 37,
    /// Natural logarithm.
    Log = 38,
    /// Square root.
    Sqrt = 39,
    /// Cube root.
    Cbrt = 40,
    /// Ceiling.
    Ceil = 41,
    /// Floor.
    Floor = 42,
    /// Absolute value.
    Abs = 43,
    /// Round to integer.
    Rint = 44,
    /// Bitwise invert.
    BitInvert = 45,
    /// Logical NOT.
    Not = 46,
    /// Cast to int64.
    CastToInt64 = 47,
    /// Cast to uint64.
    CastToUint64 = 48,
    /// Cast to float64.
    CastToFloat64 = 49,
}

/// NaN comparison policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum NanEquality {
    /// All NaN values compare equal.
    AllEqual = 0,
    /// NaN values do not compare equal.
    Unequal = 1,
}

/// Duplicate row retention policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum DuplicateKeepOption {
    /// Keep an unspecified duplicate occurrence.
    KeepAny = 0,
    /// Keep the first duplicate occurrence.
    KeepFirst = 1,
    /// Keep the last duplicate occurrence.
    KeepLast = 2,
    /// Remove all duplicate occurrences.
    KeepNone = 3,
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
    /// Null-aware equality.
    NullEquals = 27,
    /// Null-aware inequality.
    NullNotEquals = 28,
    /// Null-aware maximum.
    NullMax = 29,
    /// Null-aware minimum.
    NullMin = 30,
    /// Generic binary operation generated from PTX.
    GenericBinary = 31,
    /// Null-aware logical AND.
    NullLogicalAnd = 32,
    /// Null-aware logical OR.
    NullLogicalOr = 33,
    /// Invalid binary operation sentinel.
    InvalidBinary = 34,
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

/// Arrow C Device Data Interface device types
///
/// These values correspond to the device types defined in the Arrow C Device
/// Data Interface specification.
///
/// See: <https://arrow.apache.org/docs/format/CDeviceDataInterface.html>
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ArrowDeviceType {
    /// CPU memory.
    Cpu = 1,
    /// CUDA device memory.
    Cuda = 2,
    /// CUDA-pinned host memory.
    CudaHost = 3,
    /// OpenCL device memory.
    OpenCL = 4,
    /// Vulkan device memory.
    Vulkan = 7,
    /// Metal device memory.
    Metal = 8,
    /// NVIDIA VPI device memory.
    Vpi = 9,
    /// ROCm device memory.
    Rocm = 10,
    /// ROCm-pinned host memory.
    RocmHost = 11,
    /// Extension-defined device memory.
    ExtDev = 12,
    /// CUDA managed memory.
    CudaManaged = 13,
    /// oneAPI device memory.
    OneAPI = 14,
    /// WebGPU device memory.
    WebGPU = 15,
    /// Hexagon device memory.
    Hexagon = 16,
}

impl ArrowDeviceType {
    /// Device ID for CPU devices (-1 indicates no specific device)
    pub const CPU_DEVICE_ID: i64 = -1;
}

impl From<ArrowDeviceType> for i32 {
    fn from(dt: ArrowDeviceType) -> i32 {
        dt as i32
    }
}

impl ArrowDeviceArray {
    /// Create a new CPU-resident ArrowDeviceArray
    ///
    /// Initializes an empty array structure for CPU (host) memory.
    /// The device_id is set to -1 (no specific device) and device_type to CPU.
    pub fn new_cpu() -> Self {
        Self {
            array: arrow::ffi::FFI_ArrowArray::empty(),
            device_id: ArrowDeviceType::CPU_DEVICE_ID,
            device_type: ArrowDeviceType::Cpu.into(),
            sync_event: std::ptr::null_mut(),
            reserved: [0; 3],
        }
    }

    /// Create a new CUDA device ArrowDeviceArray
    ///
    /// Initializes an empty array structure for CUDA GPU memory.
    ///
    /// # Arguments
    ///
    /// * `device_id` - The CUDA device ID (0, 1, 2, etc.)
    pub fn new_cuda(device_id: i64) -> Self {
        Self {
            array: arrow::ffi::FFI_ArrowArray::empty(),
            device_id,
            device_type: ArrowDeviceType::Cuda.into(),
            sync_event: std::ptr::null_mut(),
            reserved: [0; 3],
        }
    }

    /// Sets the array field (builder pattern)
    ///
    /// # Arguments
    ///
    /// * `array` - The Arrow C FFI array structure
    pub fn with_array(mut self, array: FFI_ArrowArray) -> Self {
        self.array = array;
        self
    }

    /// Sets the sync event field (builder pattern)
    ///
    /// # Arguments
    ///
    /// * `sync_event` - Pointer to a synchronization event (e.g., CUDA event)
    pub fn with_sync_event(mut self, sync_event: *mut std::ffi::c_void) -> Self {
        self.sync_event = sync_event;
        self
    }

    /// Sets the device ID field (builder pattern)
    ///
    /// Useful for changing the device ID after initial construction.
    ///
    /// # Arguments
    ///
    /// * `device_id` - The device ID (-1 for CPU, 0+ for GPU devices)
    pub fn with_device_id(mut self, device_id: i64) -> Self {
        self.device_id = device_id;
        self
    }
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

/// SAFETY: AstExpressionTree owns immutable expression configuration after construction.
unsafe impl Send for ffi::AstExpressionTree {}

/// SAFETY: AstExpressionTree can be safely accessed from multiple threads after construction.
unsafe impl Sync for ffi::AstExpressionTree {}

/// SAFETY: ReduceAggregation is a configuration object with no thread-local state.
unsafe impl Send for ffi::ReduceAggregation {}

/// SAFETY: ReduceAggregation can be safely accessed from multiple threads.
unsafe impl Sync for ffi::ReduceAggregation {}

/// SAFETY: GroupByAggregation is a configuration object with no thread-local state.
unsafe impl Send for ffi::GroupByAggregation {}

/// SAFETY: GroupByAggregation can be safely accessed from multiple threads.
unsafe impl Sync for ffi::GroupByAggregation {}

/// SAFETY: GroupBy configuration can be sent between threads.
unsafe impl Send for ffi::GroupBy {}

/// SAFETY: GroupBy configuration can be accessed from multiple threads.
unsafe impl Sync for ffi::GroupBy {}

/// SAFETY: HashJoin owns its cuDF state and is only moved across threads.
unsafe impl Send for ffi::HashJoin {}

/// SAFETY: FilteredJoin owns its cuDF state and is only moved across threads.
unsafe impl Send for ffi::FilteredJoin {}

/// SAFETY: DeviceIndexVector owns device memory and can be moved between threads.
unsafe impl Send for ffi::DeviceIndexVector {}

/// SAFETY: DeviceIndexVector exposes immutable views over owned device memory.
unsafe impl Sync for ffi::DeviceIndexVector {}

/// SAFETY: AggregationRequest configuration can be sent between threads.
unsafe impl Send for ffi::AggregationRequest {}

/// SAFETY: AggregationRequest configuration can be accessed from multiple threads.
unsafe impl Sync for ffi::AggregationRequest {}

/// SAFETY: AggregationRequests owns request configuration and can be sent between threads.
unsafe impl Send for ffi::AggregationRequests {}

/// SAFETY: AggregationRequests can be safely accessed from multiple threads during aggregation.
unsafe impl Sync for ffi::AggregationRequests {}

/// SAFETY: GroupByResult contains GPU data that can be sent between threads.
unsafe impl Send for ffi::GroupByResult {}

/// SAFETY: GroupByResult can be accessed from multiple threads.
unsafe impl Sync for ffi::GroupByResult {}

/// SAFETY: CUDA stream handles can be transferred between threads.
unsafe impl Send for ffi::CudaStream {}

/// SAFETY: Shared references to the opaque stream wrapper are safe.
unsafe impl Sync for ffi::CudaStream {}

/// SAFETY: CUDA stream refs are non-owning handles that can be transferred between threads.
unsafe impl Send for ffi::CudaStreamRef {}

/// SAFETY: Shared references to the opaque stream ref wrapper are safe.
unsafe impl Sync for ffi::CudaStreamRef {}

/// SAFETY: CUDA event handles can be transferred between threads.
unsafe impl Send for ffi::CudaEvent {}

/// SAFETY: Shared references to the opaque event wrapper are safe.
unsafe impl Sync for ffi::CudaEvent {}

/// SAFETY: The cuda memory resource is a process-global allocator; the wrapper
/// just owns its `unique_ptr` and is safe to move and share across threads.
unsafe impl Send for ffi::CudaMemoryResource {}
unsafe impl Sync for ffi::CudaMemoryResource {}

/// SAFETY: Same as `CudaMemoryResource`. Once installed via
/// `set_current_device_resource_ref`, RMM serializes its own concurrent allocate /
/// deallocate, so shared references to the handle are safe.
unsafe impl Send for ffi::PoolMemoryResource {}
unsafe impl Sync for ffi::PoolMemoryResource {}

/// SAFETY: `HostDeviceAsyncResourceRef` is a non-owning handle to the
/// process-global pinned MR. The handle itself is movable across threads,
/// and the cuDF MR behind it serializes its own concurrent allocate /
/// deallocate, so shared references are safe.
unsafe impl Send for ffi::HostDeviceAsyncResourceRef {}
unsafe impl Sync for ffi::HostDeviceAsyncResourceRef {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi::ColumnView;
    use arrow::array::{make_array, Array, Decimal128Array, Int32Array, RecordBatch, StructArray};
    use arrow::ffi::{from_ffi, from_ffi_and_data_type, FFI_ArrowArray};
    use arrow::util::pretty::{pretty_format_batches, pretty_format_columns};
    use arrow_schema::ffi::FFI_ArrowSchema;
    use arrow_schema::{DataType, Field, Schema};
    use insta::assert_snapshot;
    use std::fmt::Display;
    use std::sync::Arc;

    const CUDA_STREAM_FLAG_SYNC_DEFAULT: u32 = 0;
    const CUDA_STREAM_FLAG_NON_BLOCKING: u32 = 1;
    const CUDA_EVENT_FLAG_DEFAULT: u32 = 0;

    fn expect_stream(stream: &cxx::UniquePtr<ffi::CudaStreamView>) -> &ffi::CudaStreamView {
        stream.as_ref().expect("default stream should not be null")
    }

    fn expect_resource(
        resource: &cxx::UniquePtr<ffi::DeviceAsyncResourceRef>,
    ) -> &ffi::DeviceAsyncResourceRef {
        resource
            .as_ref()
            .expect("current device resource should not be null")
    }

    fn expect_stream_and_resource<'a>(
        stream: &'a cxx::UniquePtr<ffi::CudaStreamView>,
        resource: &'a cxx::UniquePtr<ffi::DeviceAsyncResourceRef>,
    ) -> (&'a ffi::CudaStreamView, &'a ffi::DeviceAsyncResourceRef) {
        (expect_stream(stream), expect_resource(resource))
    }

    fn default_stream_and_resource() -> (
        cxx::UniquePtr<ffi::CudaStreamView>,
        cxx::UniquePtr<ffi::DeviceAsyncResourceRef>,
    ) {
        (
            ffi::get_default_stream(),
            ffi::get_current_device_resource_ref(),
        )
    }

    #[test]
    fn test_source_info_file_path_count() {
        let single = ffi::source_info_from_file_path("../testdata/weather/result-000000.parquet");
        assert_eq!(single.num_sources(), 1);

        let multiple = ffi::source_info_from_file_paths(vec![
            "../testdata/weather/result-000000.parquet".to_string(),
            "../testdata/weather/result-000001.parquet".to_string(),
        ]);
        assert_eq!(multiple.num_sources(), 2);
    }

    #[test]
    fn test_sink_info_file_path_count() {
        let single = ffi::sink_info_from_file_path("../testdata/weather/result-000000.parquet");
        assert_eq!(single.num_sinks(), 1);

        let multiple = ffi::sink_info_from_file_paths(vec![
            "../testdata/weather/result-000000.parquet".to_string(),
            "../testdata/weather/result-000001.parquet".to_string(),
        ]);
        assert_eq!(multiple.num_sinks(), 2);
    }

    #[test]
    fn test_read_parquet_with_options_metadata() -> Result<(), Box<dyn std::error::Error>> {
        let source = ffi::source_info_from_file_paths(vec![
            "../testdata/weather/result-000000.parquet".to_string(),
            "../testdata/weather/result-000001.parquet".to_string(),
        ]);
        let options = ffi::parquet_reader_options_create(
            source.as_ref().expect("source_info should not be null"),
        );
        let stream = ffi::get_default_stream();
        let mr = ffi::get_current_device_resource_ref();

        let mut result = ffi::read_parquet(
            options
                .as_ref()
                .expect("parquet_reader_options should not be null"),
            stream.as_ref().expect("default stream should not be null"),
            mr.as_ref().expect("device resource should not be null"),
        )?;

        let metadata = result.pin_mut().release_metadata()?;
        assert!(result.pin_mut().release_metadata().is_err());
        assert_eq!(metadata.num_rows_per_source_len(), 2);
        assert!(metadata.num_rows_per_source(0)? > 0);
        assert!(metadata.num_rows_per_source(1)? > 0);
        assert!(metadata.num_input_row_groups() > 0);
        assert!(!metadata.user_data().is_null());
        assert_eq!(metadata.per_file_user_data_len(), 2);
        assert!(metadata.schema_info_len() > 0);
        let first_column = metadata.schema_info(0)?;
        assert!(!first_column.name().is_empty());
        if first_column.has_is_nullable() {
            let _ = first_column.is_nullable()?;
        }
        if first_column.has_is_binary() {
            let _ = first_column.is_binary()?;
        }
        if first_column.has_type_length() {
            let _ = first_column.type_length()?;
        }
        if metadata.has_num_row_groups_after_stats_filter() {
            let _ = metadata.num_row_groups_after_stats_filter()?;
        }
        if metadata.has_num_row_groups_after_bloom_filter() {
            let _ = metadata.num_row_groups_after_bloom_filter()?;
        }

        let table = result.pin_mut().release_table()?;
        assert!(result.pin_mut().release_table().is_err());
        assert!(table.num_rows()? > 0);

        Ok(())
    }

    // Sorting tests
    #[test]
    fn test_sort_table_ascending() -> Result<(), Box<dyn std::error::Error>> {
        let (stream, resource) = default_stream_and_resource();
        let (stream_view, resource_ref) = expect_stream_and_resource(&stream, &resource);
        let table = read_parquet_table(
            "../testdata/weather/result-000000.parquet",
            stream_view,
            resource_ref,
        )?;
        let table_view = unsafe { table.view() }?;

        let num_cols = usize::try_from(table.num_columns()?)?;
        let column_order: Vec<i32> = vec![Order::Ascending as i32; num_cols];
        let null_precedence: Vec<i32> = vec![NullOrder::Before as i32; num_cols];

        let sorted_table = ffi::sort(
            &table_view,
            &column_order,
            &null_precedence,
            stream_view,
            resource_ref,
        )?;

        assert_eq!(sorted_table.num_rows()?, table.num_rows()?);
        assert_eq!(sorted_table.num_columns()?, table.num_columns()?);
        assert_snapshot!(pretty_table(&*unsafe { sorted_table.view() }?)?);

        Ok(())
    }

    #[test]
    fn test_sort_table_descending() -> Result<(), Box<dyn std::error::Error>> {
        let (stream, resource) = default_stream_and_resource();
        let (stream_view, resource_ref) = expect_stream_and_resource(&stream, &resource);
        let table = read_parquet_table(
            "../testdata/weather/result-000000.parquet",
            stream_view,
            resource_ref,
        )?;
        let table_view = unsafe { table.view() }?;

        let num_cols = usize::try_from(table.num_columns()?)?;
        let column_order: Vec<i32> = vec![Order::Descending as i32; num_cols];
        let null_precedence: Vec<i32> = vec![NullOrder::After as i32; num_cols];

        let sorted_table = ffi::sort(
            &table_view,
            &column_order,
            &null_precedence,
            stream_view,
            resource_ref,
        )?;

        assert_eq!(sorted_table.num_rows()?, table.num_rows()?);
        assert_eq!(sorted_table.num_columns()?, table.num_columns()?);
        assert_snapshot!(pretty_table(&*unsafe { sorted_table.view() }?)?);

        Ok(())
    }

    #[test]
    fn test_stable_sort_table() -> Result<(), Box<dyn std::error::Error>> {
        let (stream, resource) = default_stream_and_resource();
        let (stream_view, resource_ref) = expect_stream_and_resource(&stream, &resource);
        let table = read_parquet_table(
            "../testdata/weather/result-000000.parquet",
            stream_view,
            resource_ref,
        )?;
        let table_view = unsafe { table.view() }?;

        let num_cols = usize::try_from(table.num_columns()?)?;
        let column_order: Vec<i32> = vec![Order::Ascending as i32; num_cols];
        let null_precedence: Vec<i32> = vec![NullOrder::Before as i32; num_cols];

        let sorted_table = ffi::stable_sort(
            &table_view,
            &column_order,
            &null_precedence,
            stream_view,
            resource_ref,
        )?;

        assert_eq!(sorted_table.num_rows()?, table.num_rows()?);
        assert_eq!(sorted_table.num_columns()?, table.num_columns()?);
        assert_snapshot!(pretty_table(&*unsafe { sorted_table.view() }?)?);

        Ok(())
    }

    #[test]
    fn test_sorted_order() -> Result<(), Box<dyn std::error::Error>> {
        let (stream, resource) = default_stream_and_resource();
        let (stream_view, resource_ref) = expect_stream_and_resource(&stream, &resource);
        let table = read_parquet_table(
            "../testdata/weather/result-000000.parquet",
            stream_view,
            resource_ref,
        )?;
        let table_view = unsafe { table.view() }?;

        let num_cols = usize::try_from(table.num_columns()?)?;
        let column_order: Vec<i32> = vec![Order::Ascending as i32; num_cols];
        let null_precedence: Vec<i32> = vec![NullOrder::Before as i32; num_cols];

        let indices = ffi::sorted_order(
            &table_view,
            &column_order,
            &null_precedence,
            stream_view,
            resource_ref,
        )?;

        assert_eq!(indices.size()?, table.num_rows()?);
        assert_snapshot!(pretty_column(
            &*unsafe { indices.view() }?,
            DataType::Int32
        )?);

        Ok(())
    }

    #[test]
    fn test_is_sorted() -> Result<(), Box<dyn std::error::Error>> {
        let (stream, resource) = default_stream_and_resource();
        let (stream_view, resource_ref) = expect_stream_and_resource(&stream, &resource);
        let table = read_parquet_table(
            "../testdata/weather/result-000000.parquet",
            stream_view,
            resource_ref,
        )?;
        let table_view = unsafe { table.view() }?;

        let num_cols = usize::try_from(table.num_columns()?)?;
        let column_order: Vec<i32> = vec![Order::Ascending as i32; num_cols];
        let null_precedence: Vec<i32> = vec![NullOrder::Before as i32; num_cols];

        let sorted_table = ffi::sort(
            &table_view,
            &column_order,
            &null_precedence,
            stream_view,
            resource_ref,
        )?;
        let sorted_view = unsafe { sorted_table.view() }?;

        let is_sorted = ffi::is_sorted(&sorted_view, &column_order, &null_precedence, stream_view)?;
        assert!(is_sorted, "Table should be sorted after calling sort_table");

        Ok(())
    }

    #[test]
    fn test_sort_by_key() -> Result<(), Box<dyn std::error::Error>> {
        let (stream, resource) = default_stream_and_resource();
        let (stream_view, resource_ref) = expect_stream_and_resource(&stream, &resource);
        let table = read_parquet_table(
            "../testdata/weather/result-000000.parquet",
            stream_view,
            resource_ref,
        )?;
        let table_view = unsafe { table.view() }?;

        let keys_view = table_view.select(&[0])?;

        let column_order = vec![Order::Ascending as i32];
        let null_precedence = vec![NullOrder::Before as i32];

        let sorted_table = ffi::sort_by_key(
            &table_view,
            &keys_view,
            &column_order,
            &null_precedence,
            stream_view,
            resource_ref,
        )?;

        assert_eq!(sorted_table.num_rows()?, table.num_rows()?);
        assert_eq!(sorted_table.num_columns()?, table.num_columns()?);
        assert_snapshot!(pretty_table(&*unsafe { sorted_table.view() }?)?);

        Ok(())
    }

    #[test]
    fn test_stable_sort_by_key() -> Result<(), Box<dyn std::error::Error>> {
        let (stream, resource) = default_stream_and_resource();
        let (stream_view, resource_ref) = expect_stream_and_resource(&stream, &resource);
        let table = read_parquet_table(
            "../testdata/weather/result-000000.parquet",
            stream_view,
            resource_ref,
        )?;
        let table_view = unsafe { table.view() }?;

        let keys_view = table_view.select(&[0, 1])?;

        let column_order = vec![Order::Ascending as i32, Order::Descending as i32];
        let null_precedence = vec![NullOrder::Before as i32, NullOrder::After as i32];

        let sorted_table = ffi::stable_sort_by_key(
            &table_view,
            &keys_view,
            &column_order,
            &null_precedence,
            stream_view,
            resource_ref,
        )?;

        assert_eq!(sorted_table.num_rows()?, table.num_rows()?);
        assert_eq!(sorted_table.num_columns()?, table.num_columns()?);
        assert_snapshot!(pretty_table(&*unsafe { sorted_table.view() }?)?);

        Ok(())
    }

    // Binary operation tests
    #[test]
    fn test_binary_op_col_col_add() -> Result<(), Box<dyn std::error::Error>> {
        let (stream, resource) = default_stream_and_resource();
        let (stream_view, resource_ref) = expect_stream_and_resource(&stream, &resource);
        let table = read_parquet_table(
            "../testdata/weather/result-000000.parquet",
            stream_view,
            resource_ref,
        )?;
        let table_view = unsafe { table.view() }?;

        let col1 = table_view.column(1)?;
        let col2 = table_view.column(2)?;

        let output_type = ffi::new_data_type(TypeId::Float64 as i32);
        let result = ffi::binary_operation_column_column(
            &col1,
            &col2,
            BinaryOperator::Add as i32,
            &output_type,
            stream_view,
            resource_ref,
        )?;

        assert_eq!(result.size()?, col1.size()?);
        assert_eq!(result.size()?, col2.size()?);
        assert_snapshot!(pretty_column(
            &*unsafe { result.view() }?,
            DataType::Float64
        )?);

        Ok(())
    }

    #[test]
    fn test_binary_op_col_col_multiply() -> Result<(), Box<dyn std::error::Error>> {
        let (stream, resource) = default_stream_and_resource();
        let (stream_view, resource_ref) = expect_stream_and_resource(&stream, &resource);
        let table = read_parquet_table(
            "../testdata/weather/result-000000.parquet",
            stream_view,
            resource_ref,
        )?;
        let table_view = unsafe { table.view() }?;

        let col1 = table_view.column(1)?;
        let col2 = table_view.column(2)?;

        let output_type = ffi::new_data_type(TypeId::Float64 as i32);
        let result = ffi::binary_operation_column_column(
            &col1,
            &col2,
            BinaryOperator::Mul as i32,
            &output_type,
            stream_view,
            resource_ref,
        )?;

        assert_eq!(result.size()?, col1.size()?);
        assert_snapshot!(pretty_column(
            &*unsafe { result.view() }?,
            DataType::Float64
        )?);

        Ok(())
    }

    #[test]
    fn binary_operation_accepts_ptx_udf_overload() -> Result<(), Box<dyn std::error::Error>> {
        let table = table_from_i32_columns(&[("lhs", vec![1, 2, 3]), ("rhs", vec![10, 20, 30])])?;
        let table_view = unsafe { table.view() }?;
        let lhs = table_view.column(0)?;
        let rhs = table_view.column(1)?;
        let output_type = ffi::new_data_type(TypeId::Int64 as i32);
        let (stream, resource) = default_stream_and_resource();
        let (stream_view, resource_ref) = expect_stream_and_resource(&stream, &resource);
        // cuDF exposes a binary_operation overload that takes a PTX device
        // function instead of a binary_operator enum. The inline PTX below is
        // only a parity fixture for that overload; it computes lhs^3 + rhs.
        let ptx = r#"
.version 6.4
.target sm_70
.address_size 64

.visible .func (.param .b32 result) add_cube(
    .param .b64 output,
    .param .b32 lhs,
    .param .b32 rhs
)
{
    .reg .b32 %r<3>;
    .reg .b64 %rd<7>;
    ld.param.u64 %rd1, [output];
    ld.param.s32 %r1, [lhs];
    cvt.s64.s32 %rd2, %r1;
    mul.wide.s32 %rd3, %r1, %r1;
    mul.lo.s64 %rd4, %rd3, %rd2;
    ld.param.s32 %r2, [rhs];
    cvt.s64.s32 %rd5, %r2;
    add.s64 %rd6, %rd4, %rd5;
    st.u64 [%rd1], %rd6;
    mov.u32 %r2, 0;
    st.param.b32 [result], %r2;
    ret;
}
"#;

        let result =
            ffi::binary_operation_ptx(&lhs, &rhs, ptx, &output_type, stream_view, resource_ref)?;
        assert_snapshot!(pretty_column(&*unsafe { result.view() }?, DataType::Int64)?, @r"
        +------+
        | test |
        +------+
        | 11   |
        | 28   |
        | 57   |
        +------+
        ");
        Ok(())
    }

    #[test]
    fn test_binary_operators_enum() {
        let rust_values = [
            BinaryOperator::Add as i32,
            BinaryOperator::Sub as i32,
            BinaryOperator::Mul as i32,
            BinaryOperator::Div as i32,
            BinaryOperator::TrueDiv as i32,
            BinaryOperator::FloorDiv as i32,
            BinaryOperator::Mod as i32,
            BinaryOperator::PMod as i32,
            BinaryOperator::PyMod as i32,
            BinaryOperator::Pow as i32,
            BinaryOperator::IntPow as i32,
            BinaryOperator::LogBase as i32,
            BinaryOperator::Atan2 as i32,
            BinaryOperator::ShiftLeft as i32,
            BinaryOperator::ShiftRight as i32,
            BinaryOperator::ShiftRightUnsigned as i32,
            BinaryOperator::BitwiseAnd as i32,
            BinaryOperator::BitwiseOr as i32,
            BinaryOperator::BitwiseXor as i32,
            BinaryOperator::LogicalAnd as i32,
            BinaryOperator::LogicalOr as i32,
            BinaryOperator::Equal as i32,
            BinaryOperator::NotEqual as i32,
            BinaryOperator::Less as i32,
            BinaryOperator::Greater as i32,
            BinaryOperator::LessEqual as i32,
            BinaryOperator::GreaterEqual as i32,
            BinaryOperator::NullEquals as i32,
            BinaryOperator::NullNotEquals as i32,
            BinaryOperator::NullMax as i32,
            BinaryOperator::NullMin as i32,
            BinaryOperator::GenericBinary as i32,
            BinaryOperator::NullLogicalAnd as i32,
            BinaryOperator::NullLogicalOr as i32,
            BinaryOperator::InvalidBinary as i32,
        ];

        assert_eq!(
            rust_values,
            [
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34,
            ]
        );
    }

    #[test]
    fn operation_policy_enums_match_cudf() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!([Order::Ascending as i32, Order::Descending as i32], [0, 1]);
        assert_eq!([NullOrder::After as i32, NullOrder::Before as i32], [0, 1]);
        assert_eq!(
            [
                OutOfBoundsPolicy::Nullify as i32,
                OutOfBoundsPolicy::DontCheck as i32,
            ],
            [0, 1]
        );
        assert_eq!(
            [NullEquality::Equal as i32, NullEquality::Unequal as i32],
            [0, 1]
        );
        assert_eq!(
            [NanEquality::AllEqual as i32, NanEquality::Unequal as i32],
            [0, 1]
        );
        assert_eq!(
            [
                DuplicateKeepOption::KeepAny as i32,
                DuplicateKeepOption::KeepFirst as i32,
                DuplicateKeepOption::KeepLast as i32,
                DuplicateKeepOption::KeepNone as i32,
            ],
            [0, 1, 2, 3]
        );
        assert_eq!(
            [
                AstTableReference::Left as i32,
                AstTableReference::Right as i32,
                AstTableReference::Output as i32,
            ],
            [0, 1, 2]
        );
        assert_eq!(
            [
                AstOperator::Add as i32,
                AstOperator::Sub as i32,
                AstOperator::Mul as i32,
                AstOperator::Div as i32,
                AstOperator::TrueDiv as i32,
                AstOperator::FloorDiv as i32,
                AstOperator::Mod as i32,
                AstOperator::PyMod as i32,
                AstOperator::Pow as i32,
                AstOperator::Equal as i32,
                AstOperator::NullEqual as i32,
                AstOperator::NotEqual as i32,
                AstOperator::Less as i32,
                AstOperator::Greater as i32,
                AstOperator::LessEqual as i32,
                AstOperator::GreaterEqual as i32,
                AstOperator::BitwiseAnd as i32,
                AstOperator::BitwiseOr as i32,
                AstOperator::BitwiseXor as i32,
                AstOperator::LogicalAnd as i32,
                AstOperator::NullLogicalAnd as i32,
                AstOperator::LogicalOr as i32,
                AstOperator::NullLogicalOr as i32,
                AstOperator::Identity as i32,
                AstOperator::IsNull as i32,
                AstOperator::Sin as i32,
                AstOperator::Cos as i32,
                AstOperator::Tan as i32,
                AstOperator::ArcSin as i32,
                AstOperator::ArcCos as i32,
                AstOperator::ArcTan as i32,
                AstOperator::Sinh as i32,
                AstOperator::Cosh as i32,
                AstOperator::Tanh as i32,
                AstOperator::ArcSinh as i32,
                AstOperator::ArcCosh as i32,
                AstOperator::ArcTanh as i32,
                AstOperator::Exp as i32,
                AstOperator::Log as i32,
                AstOperator::Sqrt as i32,
                AstOperator::Cbrt as i32,
                AstOperator::Ceil as i32,
                AstOperator::Floor as i32,
                AstOperator::Abs as i32,
                AstOperator::Rint as i32,
                AstOperator::BitInvert as i32,
                AstOperator::Not as i32,
                AstOperator::CastToInt64 as i32,
                AstOperator::CastToUint64 as i32,
                AstOperator::CastToFloat64 as i32,
            ],
            std::array::from_fn(|value| value as i32)
        );
        Ok(())
    }

    #[test]
    fn test_type_id_enum() {
        assert_eq!(TypeId::Int8 as i32, 1);
        assert_eq!(TypeId::Int32 as i32, 3);
        assert_eq!(TypeId::Float32 as i32, 9);
        assert_eq!(TypeId::Float64 as i32, 10);
    }

    #[test]
    fn arrow_device_type_matches_c_device_interface() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            [
                ArrowDeviceType::Cpu as i32,
                ArrowDeviceType::Cuda as i32,
                ArrowDeviceType::CudaHost as i32,
                ArrowDeviceType::OpenCL as i32,
                ArrowDeviceType::Vulkan as i32,
                ArrowDeviceType::Metal as i32,
                ArrowDeviceType::Vpi as i32,
                ArrowDeviceType::Rocm as i32,
                ArrowDeviceType::RocmHost as i32,
                ArrowDeviceType::ExtDev as i32,
                ArrowDeviceType::CudaManaged as i32,
                ArrowDeviceType::OneAPI as i32,
                ArrowDeviceType::WebGPU as i32,
                ArrowDeviceType::Hexagon as i32,
            ],
            [1, 2, 3, 4, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        );
        Ok(())
    }

    #[test]
    fn test_reduce_uses_full_output_data_type() -> Result<(), Box<dyn std::error::Error>> {
        let table = table_from_decimal128_column("amount", vec![12345, 67890], 2)?;
        let column = unsafe { table.view() }?.column(0)?;
        let output_type = ffi::new_data_type_with_scale(TypeId::Decimal128 as i32, -2);
        let stream = ffi::get_default_stream();
        let resource = ffi::get_current_device_resource_ref();
        let (stream_view, resource_ref) = expect_stream_and_resource(&stream, &resource);

        let result = ffi::reduce(
            &column,
            &ffi::make_sum_aggregation(),
            &output_type,
            stream_view,
            resource_ref,
        )?;
        let result_type = result.data_type()?;

        assert!(result.is_valid(stream_view)?);
        assert_eq!(result_type.id(), TypeId::Decimal128 as i32);
        assert_eq!(result_type.scale(), -2);

        Ok(())
    }

    #[test]
    fn test_reduce_with_init() -> Result<(), Box<dyn std::error::Error>> {
        let table = table_from_i32_columns(&[("values", vec![1, 2, 3])])?;
        let values = unsafe { table.view() }?.column(0)?;
        let init_table = table_from_i32_columns(&[("init", vec![10])])?;
        let (stream, resource) = default_stream_and_resource();
        let (stream_view, resource_ref) = expect_stream_and_resource(&stream, &resource);
        let init = ffi::get_element(
            &*unsafe { init_table.view() }?.column(0)?,
            0,
            stream_view,
            resource_ref,
        )?;
        let output_type = ffi::new_data_type(TypeId::Int32 as i32);

        let result = ffi::reduce_with_init(
            &values,
            &ffi::make_sum_aggregation(),
            &output_type,
            &init,
            stream_view,
            resource_ref,
        )?;
        let result_column = ffi::make_column_from_scalar(&result, 1, stream_view, resource_ref)?;

        assert_snapshot!(pretty_column(&*unsafe { result_column.view() }?, DataType::Int32)?, @r"
        +------+
        | test |
        +------+
        | 16   |
        +------+
        ");

        Ok(())
    }

    #[test]
    fn test_count_aggregation_respects_null_policy() -> Result<(), Box<dyn std::error::Error>> {
        let table = table_from_nullable_i32_column("values", vec![Some(1), None, Some(3), None])?;
        let values = unsafe { table.view() }?.column(0)?;
        let output_type = ffi::new_data_type(TypeId::Int32 as i32);
        let (stream, resource) = default_stream_and_resource();
        let (stream_view, resource_ref) = expect_stream_and_resource(&stream, &resource);

        let exclude = ffi::reduce(
            &values,
            &ffi::make_count_aggregation(NullPolicy::Exclude as i32),
            &output_type,
            stream_view,
            resource_ref,
        )?;
        let include = ffi::reduce(
            &values,
            &ffi::make_count_aggregation(NullPolicy::Include as i32),
            &output_type,
            stream_view,
            resource_ref,
        )?;

        let exclude_column = ffi::make_column_from_scalar(&exclude, 1, stream_view, resource_ref)?;
        let include_column = ffi::make_column_from_scalar(&include, 1, stream_view, resource_ref)?;

        assert_snapshot!(pretty_column(&*unsafe { exclude_column.view() }?, DataType::Int32)?, @r"
        +------+
        | test |
        +------+
        | 2    |
        +------+
        ");
        assert_snapshot!(pretty_column(&*unsafe { include_column.view() }?, DataType::Int32)?, @r"
        +------+
        | test |
        +------+
        | 4    |
        +------+
        ");

        Ok(())
    }

    #[test]
    fn test_join_enum_parity() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(ffi::join_no_match(), i32::MIN);
        assert_eq!(NullEquality::Equal as i32, 0);
        assert_eq!(NullEquality::Unequal as i32, 1);
        assert_eq!(JoinKind::Inner as i32, 0);
        assert_eq!(JoinKind::Left as i32, 1);
        assert_eq!(JoinKind::Full as i32, 2);
        assert_eq!(JoinKind::LeftSemi as i32, 3);
        assert_eq!(JoinKind::LeftAnti as i32, 4);
        assert_eq!(SetAsBuildTable::Left as i32, 0);
        assert_eq!(SetAsBuildTable::Right as i32, 1);
        Ok(())
    }

    #[test]
    fn test_ast_filter_join_indices() -> Result<(), Box<dyn std::error::Error>> {
        let left =
            table_from_i32_columns(&[("key", vec![1, 2, 2, 3]), ("val", vec![10, 20, 25, 30])])?;
        let right = table_from_i32_columns(&[("key", vec![2, 2, 3]), ("val", vec![15, 30, 35])])?;
        let left_view = unsafe { left.view() }?;
        let right_view = unsafe { right.view() }?;
        let left_keys = left_view.select(&[0])?;
        let right_keys = right_view.select(&[0])?;
        let stream = ffi::get_default_stream();
        let resource = ffi::get_current_device_resource_ref();
        let (stream_view, resource_ref) = expect_stream_and_resource(&stream, &resource);
        let mut indices = ffi::inner_join_indices(
            &left_keys,
            &right_keys,
            NullEquality::Equal as i32,
            stream_view,
            resource_ref,
        )?;
        let left_indices = indices.pin_mut().release_left();
        let right_indices = indices.pin_mut().release_right();
        let left_indices_view = unsafe { left_indices.view() };

        let mut predicate = ffi::ast_expression_tree_create();
        let left_val = ffi::ast_expression_tree_add_column_reference(
            predicate.pin_mut(),
            0,
            AstTableReference::Left as i32,
        )?;
        let right_val = ffi::ast_expression_tree_add_column_reference(
            predicate.pin_mut(),
            0,
            AstTableReference::Right as i32,
        )?;
        ffi::ast_expression_tree_add_operation(
            predicate.pin_mut(),
            AstOperator::Less as i32,
            left_val,
            right_val,
        )?;

        let left_values = left_view.select(&[1])?;
        let right_values = right_view.select(&[1])?;
        let mut filtered = ffi::filter_join_indices(
            &left_values,
            &right_values,
            left_indices
                .as_ref()
                .expect("left index vector should not be null"),
            right_indices
                .as_ref()
                .expect("right index vector should not be null"),
            &predicate,
            JoinKind::Inner as i32,
            stream_view,
            resource_ref,
        )?;
        let filtered_left = filtered.pin_mut().release_left();
        let filtered_right = filtered.pin_mut().release_right();

        assert_eq!(left_indices.size(), 5);
        assert_eq!(left_indices_view.size()?, 5);
        assert_eq!(right_indices.size(), 5);
        assert_eq!(filtered_left.size(), 3);
        assert_eq!(filtered_right.size(), 3);
        Ok(())
    }

    // Filter tests
    #[test]
    fn test_apply_boolean_mask() -> Result<(), Box<dyn std::error::Error>> {
        let (stream, resource) = default_stream_and_resource();
        let (stream_view, resource_ref) = expect_stream_and_resource(&stream, &resource);
        let table = read_parquet_table(
            "../testdata/weather/result-000000.parquet",
            stream_view,
            resource_ref,
        )?;
        let table_view = unsafe { table.view() }?;

        let min_temp = table_view.column(1)?;
        let max_temp = table_view.column(2)?;

        let output_type = ffi::new_data_type(TypeId::Bool8 as i32);
        let boolean_mask = ffi::binary_operation_column_column(
            &min_temp,
            &max_temp,
            BinaryOperator::Less as i32,
            &output_type,
            stream_view,
            resource_ref,
        )?;

        let boolean_mask_view = unsafe { boolean_mask.view() }?;
        let filtered_table =
            ffi::apply_boolean_mask(&table_view, &boolean_mask_view, stream_view, resource_ref)?;

        assert!(filtered_table.num_rows()? < table.num_rows()?);
        assert_eq!(filtered_table.num_columns()?, table.num_columns()?);

        let filtered_view = unsafe { filtered_table.view() }?;
        let filtered_col = filtered_view.column(1)?;
        assert_snapshot!(pretty_column(&filtered_col, DataType::Float64)?);

        Ok(())
    }

    #[test]
    fn test_sequence_overloads() -> Result<(), Box<dyn std::error::Error>> {
        let values = table_from_i32_columns(&[("values", vec![5, 2])])?;
        let values_view = unsafe { values.view() }?;
        let (stream, resource) = default_stream_and_resource();
        let (stream_view, resource_ref) = expect_stream_and_resource(&stream, &resource);
        let init = ffi::get_element(&*values_view.column(0)?, 0, stream_view, resource_ref)?;
        let step = ffi::get_element(&*values_view.column(0)?, 1, stream_view, resource_ref)?;

        let implicit = ffi::sequence_from(3, &init, stream_view, resource_ref)?;
        let explicit = ffi::sequence_with_step(3, &init, &step, stream_view, resource_ref)?;

        assert_snapshot!(pretty_column(&*unsafe { implicit.view() }?, DataType::Int32)?, @r"
        +------+
        | test |
        +------+
        | 5    |
        | 6    |
        | 7    |
        +------+
        ");
        assert_snapshot!(pretty_column(&*unsafe { explicit.view() }?, DataType::Int32)?, @r"
        +------+
        | test |
        +------+
        | 5    |
        | 7    |
        | 9    |
        +------+
        ");
        Ok(())
    }

    #[test]
    fn test_scatter_overloads() -> Result<(), Box<dyn std::error::Error>> {
        let target = table_from_i32_columns(&[("a", vec![0, 0, 0]), ("b", vec![0, 0, 0])])?;
        let source = table_from_i32_columns(&[("a", vec![7]), ("b", vec![8])])?;
        let indices = table_from_i32_columns(&[("indices", vec![1])])?;
        let target_view = unsafe { target.view() }?;
        let source_view = unsafe { source.view() }?;
        let indices_view = unsafe { indices.view() }?.column(0)?;
        let (stream, resource) = default_stream_and_resource();
        let (stream_view, resource_ref) = expect_stream_and_resource(&stream, &resource);

        let from_table = ffi::scatter_table(
            &source_view,
            &indices_view,
            &target_view,
            stream_view,
            resource_ref,
        )?;

        let first = ffi::get_element(&*source_view.column(0)?, 0, stream_view, resource_ref)?;
        let second = ffi::get_element(&*source_view.column(1)?, 0, stream_view, resource_ref)?;
        let scalars = [first.as_ptr(), second.as_ptr()];
        let from_scalars = ffi::scatter_scalars(
            &scalars,
            &indices_view,
            &target_view,
            stream_view,
            resource_ref,
        )?;

        assert_eq!(
            format!("{}", pretty_table(&*unsafe { from_table.view() }?)?),
            format!("{}", pretty_table(&*unsafe { from_scalars.view() }?)?)
        );
        Ok(())
    }

    // GroupBy tests
    #[test]
    fn test_groupby_sum() -> Result<(), Box<dyn std::error::Error>> {
        let (stream, resource) = default_stream_and_resource();
        let (stream_view, resource_ref) = expect_stream_and_resource(&stream, &resource);
        let table = read_parquet_table(
            "../testdata/weather/result-000000.parquet",
            stream_view,
            resource_ref,
        )?;
        let table_view = unsafe { table.view() }?;

        let column_order: &[i32] = &[];
        let null_precedence: &[i32] = &[];
        let groupby = ffi::groupby_create(
            &*table_view.select(&[21])?,
            NullPolicy::Exclude as i32,
            Sorted::No as i32,
            column_order,
            null_precedence,
        );

        let value_column = table_view.column(1)?;
        let mut request = ffi::aggregation_request_create(&value_column);
        request.pin_mut().add(ffi::make_max_aggregation_groupby());

        let mut agg_requests = ffi::aggregation_requests_create();
        agg_requests.pin_mut().add(request);
        let mut groupby_result = groupby.aggregate(&agg_requests, stream_view, resource_ref)?;

        let mut aggregation_result = groupby_result.pin_mut().release_result(0);
        let keys = groupby_result.pin_mut().release_keys();

        assert_eq!(aggregation_result.len(), 1);
        assert_eq!(
            aggregation_result.pin_mut().release(0).size()?,
            keys.num_rows()?
        );

        Ok(())
    }

    #[test]
    fn test_groupby_multiple_aggregations() -> Result<(), Box<dyn std::error::Error>> {
        let (stream, resource) = default_stream_and_resource();
        let (stream_view, resource_ref) = expect_stream_and_resource(&stream, &resource);
        let table = read_parquet_table(
            "../testdata/weather/result-000001.parquet",
            stream_view,
            resource_ref,
        )?;
        let table_view = unsafe { table.view() }?;

        let keys_view = table_view.select(&[0])?;
        let column_order: &[i32] = &[];
        let null_precedence: &[i32] = &[];
        let groupby = ffi::groupby_create(
            &keys_view,
            NullPolicy::Exclude as i32,
            Sorted::No as i32,
            column_order,
            null_precedence,
        );

        let value_column = table_view.column(1)?;
        let mut agg_request = ffi::aggregation_request_create(&value_column);
        agg_request
            .pin_mut()
            .add(ffi::make_sum_aggregation_groupby());
        agg_request
            .pin_mut()
            .add(ffi::make_min_aggregation_groupby());
        agg_request
            .pin_mut()
            .add(ffi::make_max_aggregation_groupby());

        let mut requests = ffi::aggregation_requests_create();
        requests.pin_mut().add(agg_request);
        let mut groupby_result = groupby.aggregate(&requests, stream_view, resource_ref)?;

        assert_eq!(groupby_result.len(), 1);
        let mut agg_result = groupby_result.pin_mut().release_result(0);
        assert_eq!(agg_result.len(), 3);

        let sum_column = agg_result.pin_mut().release(0);
        let min_column = agg_result.pin_mut().release(1);
        let max_column = agg_result.pin_mut().release(2);

        let keys = groupby_result.pin_mut().release_keys();
        assert!(keys.num_rows()? > 0);

        assert_eq!(sum_column.size()?, keys.num_rows()?);
        assert_eq!(min_column.size()?, keys.num_rows()?);
        assert_eq!(max_column.size()?, keys.num_rows()?);

        Ok(())
    }

    // Slice tests
    #[test]
    fn test_slice_column_basic() -> Result<(), Box<dyn std::error::Error>> {
        let (stream, resource) = default_stream_and_resource();
        let (stream_view, resource_ref) = expect_stream_and_resource(&stream, &resource);
        let table = read_parquet_table(
            "../testdata/weather/result-000000.parquet",
            stream_view,
            resource_ref,
        )?;
        let table_view = unsafe { table.view() }?;
        let original_col = table_view.column(1)?;

        let original_size = original_col.size()?;
        assert!(original_size > 10, "Need at least 10 rows for testing");

        let sliced_views =
            unsafe { ffi::slice_column(&original_col, &[5, 10, 10, 12], stream_view) }?;
        assert_eq!(sliced_views.len(), 2);
        let sliced_col = unsafe { sliced_views.get(0) }?;
        let second_slice = unsafe { sliced_views.get(1) }?;

        assert_eq!(sliced_col.size()?, 5);
        assert_eq!(second_slice.size()?, 2);

        let original_dtype = original_col.data_type()?;
        let sliced_dtype = sliced_col.data_type()?;
        assert_eq!(original_dtype.id(), sliced_dtype.id());

        assert_snapshot!(pretty_column(&sliced_col, DataType::Float64)?, @r"
        +------+
        | test |
        +------+
        | 16.9 |
        | 18.2 |
        | 17.0 |
        | 19.5 |
        | 22.8 |
        +------+
        ");

        Ok(())
    }

    #[test]
    fn test_slice_table_preserves_all_intervals() -> Result<(), Box<dyn std::error::Error>> {
        let table = table_from_i32_columns(&[("a", vec![1, 2, 3, 4, 5])])?;
        let table_view = unsafe { table.view() }?;
        let stream = ffi::get_default_stream();
        let stream_view = expect_stream(&stream);

        let slices = unsafe { ffi::slice_table(&table_view, &[0, 2, 2, 5], stream_view) }?;
        assert_eq!(slices.len(), 2);
        assert_eq!(unsafe { slices.get(0) }?.num_rows()?, 2);
        assert_eq!(unsafe { slices.get(1) }?.num_rows()?, 3);
        Ok(())
    }

    #[test]
    fn test_slice_column_from_start() -> Result<(), Box<dyn std::error::Error>> {
        let (stream, resource) = default_stream_and_resource();
        let (stream_view, resource_ref) = expect_stream_and_resource(&stream, &resource);
        let table = read_parquet_table(
            "../testdata/weather/result-000000.parquet",
            stream_view,
            resource_ref,
        )?;
        let table_view = unsafe { table.view() }?;
        let original_col = table_view.column(2)?;

        let sliced_views = unsafe { ffi::slice_column(&original_col, &[0, 10], stream_view) }?;
        assert_eq!(sliced_views.len(), 1);
        let sliced_col = unsafe { sliced_views.get(0) }?;

        assert_eq!(sliced_col.size()?, 10);
        assert_snapshot!(pretty_column(&sliced_col, DataType::Float64)?, @r"
        +------+
        | test |
        +------+
        | 0.0  |
        | 3.6  |
        | 3.6  |
        | 39.8 |
        | 2.8  |
        | 0.0  |
        | 0.2  |
        | 0.0  |
        | 0.0  |
        | 16.2 |
        +------+
        ");

        Ok(())
    }

    // CUDA Stream tests

    // Test the various methods of creating streams. Assert that each method yields a valid stream.
    #[test]
    fn test_cuda_stream_create() -> Result<(), Box<dyn std::error::Error>> {
        let stream = ffi::cuda_stream_create()?;
        assert!(stream.is_valid());
        stream.synchronize()?;

        let stream = ffi::cuda_stream_create_with_flags(CUDA_STREAM_FLAG_SYNC_DEFAULT)?;
        assert!(stream.is_valid());
        stream.synchronize()?;

        let stream = ffi::cuda_stream_create_with_flags(CUDA_STREAM_FLAG_NON_BLOCKING)?;
        assert!(stream.is_valid());
        stream.synchronize()?;

        Ok(())
    }

    #[test]
    fn test_cuda_stream_view_bindings() -> Result<(), Box<dyn std::error::Error>> {
        let stream = ffi::cuda_stream_create()?;
        let view = unsafe {
            ffi::cuda_stream_view(stream.as_ref().expect("CudaStream should not be null"))
        };
        view.synchronize()?;

        let default_view = ffi::get_default_stream();
        if ffi::is_ptds_enabled() {
            assert!(default_view.is_per_thread_default());
        } else {
            assert!(default_view.is_default());
        }
        default_view.synchronize()?;

        Ok(())
    }

    #[test]
    fn test_cuda_device_bindings() -> Result<(), Box<dyn std::error::Error>> {
        let current_device = ffi::cuda_get_device()?;
        ffi::cuda_set_device(current_device)?;
        assert_eq!(ffi::cuda_get_device()?, current_device);

        Ok(())
    }

    #[test]
    fn test_cuda_event_bindings() -> Result<(), Box<dyn std::error::Error>> {
        let producer = ffi::cuda_stream_create_with_flags(CUDA_STREAM_FLAG_NON_BLOCKING)?;
        let consumer = ffi::cuda_stream_create_with_flags(CUDA_STREAM_FLAG_NON_BLOCKING)?;
        let producer_view = unsafe {
            ffi::cuda_stream_view(producer.as_ref().expect("CudaStream should not be null"))
        };
        let consumer_view = unsafe {
            ffi::cuda_stream_view(consumer.as_ref().expect("CudaStream should not be null"))
        };
        let producer_ref = ffi::cuda_stream_ref_from_view(
            producer_view
                .as_ref()
                .expect("CudaStreamView should not be null"),
        );
        let consumer_ref = ffi::cuda_stream_ref_from_view(
            consumer_view
                .as_ref()
                .expect("CudaStreamView should not be null"),
        );

        let event =
            ffi::cuda_event_create_on_device(ffi::cuda_get_device()?, CUDA_EVENT_FLAG_DEFAULT)?;
        let event_ref = event.as_ref().expect("CudaEvent should not be null");
        assert!(event_ref.is_valid());
        event_ref.record(
            producer_ref
                .as_ref()
                .expect("CudaStreamRef should not be null"),
        )?;
        event_ref.sync()?;
        assert!(event_ref.is_done()?);

        let event = producer_ref
            .as_ref()
            .expect("CudaStreamRef should not be null")
            .record_event(CUDA_EVENT_FLAG_DEFAULT)?;
        consumer_ref
            .as_ref()
            .expect("CudaStreamRef should not be null")
            .wait_event(event.as_ref().expect("CudaEvent should not be null"))?;
        consumer.synchronize()?;
        assert!(event
            .as_ref()
            .expect("CudaEvent should not be null")
            .is_done()?);

        Ok(())
    }

    #[test]
    fn test_current_device_resource_ref_reads_parquet() -> Result<(), Box<dyn std::error::Error>> {
        let stream = ffi::get_default_stream();
        let resource = get_current_device_resource_ref();
        let (stream_view, resource_ref) = expect_stream_and_resource(&stream, &resource);
        let table = read_parquet_table(
            "../testdata/weather/result-000000.parquet",
            stream_view,
            resource_ref,
        )?;

        assert!(table.num_rows()? > 0);
        assert!(table.num_columns()? > 0);
        Ok(())
    }

    #[test]
    fn column_view_metadata_matches_cudf() -> Result<(), Box<dyn std::error::Error>> {
        let table = table_from_nullable_i32_column("a", vec![Some(1), None, Some(3)])?;
        let table_view = unsafe { table.view() }?;
        let column = table_view.column(0)?;
        let dtype = column.data_type()?;

        assert!(column.nullable()?);
        assert_eq!(column.num_children()?, 0);
        assert_ne!(column.null_mask()?, 0);
        assert!(ffi::is_fixed_width(&dtype));
        assert_eq!(ffi::size_of(&dtype)?, std::mem::size_of::<i32>());
        let string_type = ffi::new_data_type(TypeId::String as i32);
        assert!(!ffi::is_fixed_width(&string_type));
        assert!(ffi::size_of(&string_type).is_err());
        assert_eq!(ffi::bitmask_allocation_size_bytes(3, 64), 64);
        assert!(
            unsafe { table.get_column(0) }?.alloc_size()? >= 3 * std::mem::size_of::<i32>() + 64
        );
        Ok(())
    }

    #[test]
    fn arrow_metadata_preserves_all_upstream_fields() -> Result<(), Box<dyn std::error::Error>> {
        let mut metadata = ffi::make_column_metadata_with_name("amount");
        metadata.pin_mut().set_timezone("UTC");
        metadata.pin_mut().set_precision(12);
        let child = ffi::make_column_metadata_with_name("element");
        metadata.pin_mut().push_child(&child);

        assert_eq!(metadata.name(), "amount");
        assert_eq!(metadata.timezone(), "UTC");
        assert!(metadata.has_precision());
        assert_eq!(metadata.precision()?, 12);
        assert_eq!(metadata.children_len(), 1);
        assert_eq!(metadata.child(0)?.name(), "element");
        metadata.pin_mut().clear_precision();
        assert!(!metadata.has_precision());
        assert!(metadata.precision().is_err());

        let mut vector = ffi::make_column_metadata_vector();
        vector.pin_mut().push(&metadata);
        assert_eq!(vector.len(), 1);
        assert_eq!(vector.get(0)?.name(), "amount");

        let table = table_from_i32_columns(&[("ignored", vec![1, 2, 3])])?;
        let table_view = unsafe { table.view() }?;
        let mut table_metadata = ffi::get_table_metadata(&table_view)?;
        let schema_metadata = ffi::make_column_metadata_with_name("renamed");
        table_metadata.pin_mut().set(0, &schema_metadata)?;
        let mut ffi_schema = FFI_ArrowSchema::empty();
        unsafe {
            ffi::to_arrow_schema(
                &table_view,
                &table_metadata,
                &mut ffi_schema as *mut FFI_ArrowSchema as *mut u8,
            )?;
        }
        let schema = Schema::try_from(&ffi_schema)?;
        assert_eq!(schema.field(0).name(), "renamed");
        Ok(())
    }

    #[test]
    fn memory_configuration_bindings_preserve_upstream_shapes(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let memory = ffi::available_device_memory()?;
        assert!(memory.total_bytes() > 0);
        assert!(memory.free_bytes() <= memory.total_bytes());

        assert!(!ffi::make_pinned_mr_options().is_null());
        assert!(!ffi::make_pinned_mr_options_with_pool_size(1 << 20).is_null());

        let upstream = ffi::make_cuda_memory_resource();
        let default_maximum = unsafe { ffi::make_pool_memory_resource(&upstream, 256) }?;
        assert!(!unsafe { ffi::make_device_async_resource_ref(&default_maximum) }.is_null());
        let explicit_maximum =
            unsafe { ffi::make_pool_memory_resource_with_maximum(&upstream, 256, 256) }?;
        assert!(!unsafe { ffi::make_device_async_resource_ref(&explicit_maximum) }.is_null());
        Ok(())
    }

    // Test that streams are Send and Sync.
    #[test]
    fn test_cuda_stream_send_sync() -> Result<(), Box<dyn std::error::Error>> {
        let stream = Arc::new(ffi::cuda_stream_create()?);

        let handle = std::thread::spawn({
            let stream = Arc::clone(&stream);
            move || stream.is_valid()
        });

        assert!(handle.join().expect("moved stream should be valid"));
        assert!(stream.is_valid());

        Ok(())
    }

    fn table_from_i32_columns(
        columns: &[(&str, Vec<i32>)],
    ) -> Result<cxx::UniquePtr<ffi::Table>, Box<dyn std::error::Error>> {
        let schema = Schema::new(
            columns
                .iter()
                .map(|(name, _)| Field::new(*name, DataType::Int32, false))
                .collect::<Vec<_>>(),
        );
        let arrays = columns
            .iter()
            .map(|(_, values)| Arc::new(Int32Array::from(values.clone())) as _)
            .collect();
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), arrays)?;
        let struct_array = StructArray::from(batch);
        let array_data = struct_array.into_data();
        let ffi_array = FFI_ArrowArray::new(&array_data);
        let ffi_schema = FFI_ArrowSchema::try_from(schema)?;
        let device_array = ArrowDeviceArray::new_cpu().with_array(ffi_array);

        let schema_ptr = &ffi_schema as *const FFI_ArrowSchema as *const u8;
        let device_array_ptr = &device_array as *const ArrowDeviceArray as *const u8;
        let stream = ffi::get_default_stream();
        let mr = ffi::get_current_device_resource_ref();
        let (stream_view, resource_ref) = expect_stream_and_resource(&stream, &mr);
        Ok(unsafe {
            ffi::from_arrow_host(schema_ptr, device_array_ptr, stream_view, resource_ref)
        }?)
    }

    fn table_from_nullable_i32_column(
        name: &str,
        values: Vec<Option<i32>>,
    ) -> Result<cxx::UniquePtr<ffi::Table>, Box<dyn std::error::Error>> {
        let schema = Schema::new(vec![Field::new(name, DataType::Int32, true)]);
        let array = Int32Array::from(values);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(array)])?;
        let struct_array = StructArray::from(batch);
        let array_data = struct_array.into_data();
        let ffi_array = FFI_ArrowArray::new(&array_data);
        let ffi_schema = FFI_ArrowSchema::try_from(schema)?;
        let device_array = ArrowDeviceArray::new_cpu().with_array(ffi_array);

        let schema_ptr = &ffi_schema as *const FFI_ArrowSchema as *const u8;
        let device_array_ptr = &device_array as *const ArrowDeviceArray as *const u8;
        let stream = ffi::get_default_stream();
        let mr = ffi::get_current_device_resource_ref();
        let (stream_view, resource_ref) = expect_stream_and_resource(&stream, &mr);
        Ok(unsafe {
            ffi::from_arrow_host(schema_ptr, device_array_ptr, stream_view, resource_ref)
        }?)
    }

    fn table_from_decimal128_column(
        name: &str,
        values: Vec<i128>,
        scale: i8,
    ) -> Result<cxx::UniquePtr<ffi::Table>, Box<dyn std::error::Error>> {
        let data_type = DataType::Decimal128(38, scale);
        let schema = Schema::new(vec![Field::new(name, data_type.clone(), false)]);
        let array = Decimal128Array::from(values).with_precision_and_scale(38, scale)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(array)])?;
        let struct_array = StructArray::from(batch);
        let array_data = struct_array.into_data();
        let ffi_array = FFI_ArrowArray::new(&array_data);
        let ffi_schema = FFI_ArrowSchema::try_from(schema)?;
        let device_array = ArrowDeviceArray::new_cpu().with_array(ffi_array);

        let schema_ptr = &ffi_schema as *const FFI_ArrowSchema as *const u8;
        let device_array_ptr = &device_array as *const ArrowDeviceArray as *const u8;
        let stream = ffi::get_default_stream();
        let mr = ffi::get_current_device_resource_ref();
        let (stream_view, resource_ref) = expect_stream_and_resource(&stream, &mr);
        Ok(unsafe {
            ffi::from_arrow_host(schema_ptr, device_array_ptr, stream_view, resource_ref)
        }?)
    }

    fn read_parquet_table(
        file_path: &str,
        stream: &ffi::CudaStreamView,
        mr: &ffi::DeviceAsyncResourceRef,
    ) -> Result<cxx::UniquePtr<ffi::Table>, Box<dyn std::error::Error>> {
        let source = ffi::source_info_from_file_path(file_path);
        let options = ffi::parquet_reader_options_create(
            source.as_ref().expect("source_info should not be null"),
        );
        let mut result = ffi::read_parquet(
            options
                .as_ref()
                .expect("parquet_reader_options should not be null"),
            stream,
            mr,
        )?;
        Ok(result.pin_mut().release_table()?)
    }

    fn pretty_table(
        table_view: &ffi::TableView,
    ) -> Result<impl Display + use<>, Box<dyn std::error::Error>> {
        let mut device_array = ArrowDeviceArray::new_cpu();
        let mut schema = FFI_ArrowSchema::empty();
        let stream = ffi::get_default_stream();
        let resource = ffi::get_current_device_resource_ref();
        let (stream_view, resource_ref) = expect_stream_and_resource(&stream, &resource);

        let metadata = ffi::get_table_metadata(table_view)?;
        unsafe {
            ffi::to_arrow_host_table(
                table_view,
                &mut device_array as *mut ArrowDeviceArray as *mut u8,
                stream_view,
                resource_ref,
            )?;
            ffi::to_arrow_schema(
                table_view,
                &metadata,
                &mut schema as *mut FFI_ArrowSchema as *mut u8,
            )?;
        }
        let data = unsafe { from_ffi(device_array.array, &schema)? };

        let record = RecordBatch::from(StructArray::from(data));

        Ok(pretty_format_batches(&[record])?)
    }

    fn pretty_column(
        column_view: &ColumnView,
        data_type: DataType,
    ) -> Result<impl Display + use<>, Box<dyn std::error::Error>> {
        let mut device_array = ArrowDeviceArray::new_cpu();
        let stream = ffi::get_default_stream();
        let resource = ffi::get_current_device_resource_ref();
        let (stream_view, resource_ref) = expect_stream_and_resource(&stream, &resource);

        unsafe {
            let device_array_ptr = &mut device_array as *mut ArrowDeviceArray as *mut u8;
            ffi::to_arrow_host_column(column_view, device_array_ptr, stream_view, resource_ref)?;
        }
        let data = unsafe { from_ffi_and_data_type(device_array.array, data_type)? };

        let array = make_array(data);
        Ok(pretty_format_columns("test", &[array])?)
    }
}
