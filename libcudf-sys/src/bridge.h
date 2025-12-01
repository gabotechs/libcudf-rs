#pragma once

#include <memory>
#include <vector>
#include "rust/cxx.h"

// Forward declarations of Arrow C ABI types
struct ArrowSchema;
struct ArrowArray;
struct ArrowDeviceArray;

// Forward declarations of cuDF types
namespace cudf {
    class table;
    class column;
    class table_view;
    class column_view;
    struct column_metadata;
    class scalar;
    struct data_type;

    namespace io {
        struct table_with_metadata;
    }

    // Aggregation is in the global cudf namespace
    class aggregation;
    class reduce_aggregation;
    class groupby_aggregation;

    namespace groupby {
        class groupby;
        struct aggregation_request;
        struct aggregation_result;
    }
}

namespace libcudf_bridge {
    // Forward declaration
    struct Column;
    struct TableView;

    // Opaque wrapper for cuDF table
    // This allows us to safely pass cuDF objects across the FFI boundary
    struct Table {
        std::unique_ptr<cudf::table> inner;

        Table();

        ~Table();

        // Get number of columns
        [[nodiscard]] size_t num_columns() const;

        // Get number of rows
        [[nodiscard]] size_t num_rows() const;

        // Get a view of this table
        [[nodiscard]] std::unique_ptr<TableView> view() const;
    };

    // Forward declaration
    struct ColumnView;

    // Opaque wrapper for cuDF table_view
    struct TableView {
        std::unique_ptr<cudf::table_view> inner;

        TableView();

        ~TableView();

        // Get number of columns
        [[nodiscard]] size_t num_columns() const;

        // Get number of rows
        [[nodiscard]] size_t num_rows() const;

        // Select specific columns by indices
        [[nodiscard]] std::unique_ptr<TableView> select(rust::Slice<const int32_t> column_indices) const;

        // Get column view at index
        [[nodiscard]] std::unique_ptr<ColumnView> column(int32_t index) const;
    };

    // Opaque wrapper for cuDF column_view
    struct ColumnView {
        std::unique_ptr<cudf::column_view> inner;

        ColumnView();

        ~ColumnView();

        // Get number of elements
        [[nodiscard]] size_t size() const;
    };

    // Opaque wrapper for cuDF column
    struct Column {
        std::unique_ptr<cudf::column> inner;

        Column();

        ~Column();

        // Delete copy, allow move
        Column(const Column &) = delete;

        Column &operator=(const Column &) = delete;

        Column(Column &&) = default;

        Column &operator=(Column &&) = default;

        // Get number of elements
        [[nodiscard]] size_t size() const;
    };

    // Helper function to create Column from unique_ptr<cudf::column>
    Column column_from_unique_ptr(std::unique_ptr<cudf::column> col);

    // Opaque wrapper for cuDF scalar
    struct Scalar {
        std::unique_ptr<cudf::scalar> inner;

        Scalar();

        ~Scalar();

        // Check if the scalar is valid (not null)
        [[nodiscard]] bool is_valid() const;
    };

    // Opaque wrapper for cuDF aggregation
    struct Aggregation {
        std::unique_ptr<cudf::aggregation> inner;

        Aggregation();

        ~Aggregation();
    };

    // Forward declarations for methods
    struct GroupByResult;
    struct AggregationRequest;

    // Opaque wrapper for cuDF groupby
    struct GroupBy {
        std::unique_ptr<cudf::groupby::groupby> inner;

        GroupBy();

        ~GroupBy();

        // Direct cuDF method
        [[nodiscard]] std::unique_ptr<GroupByResult> aggregate(
            rust::Slice<const AggregationRequest * const> requests) const;
    };

    // Opaque wrapper for cuDF aggregation_request
    struct AggregationRequest {
        std::unique_ptr<cudf::groupby::aggregation_request> inner;

        AggregationRequest();

        ~AggregationRequest();

        // Direct cuDF method (adds aggregation to the request)
        void add(std::unique_ptr<Aggregation> agg) const;
    };

    // Direct exposure of cuDF's aggregation_result
    struct AggregationResult {
        std::vector<Column> results;

        AggregationResult();

        ~AggregationResult();

        // Delete copy, allow move
        AggregationResult(const AggregationResult &) = delete;

        AggregationResult &operator=(const AggregationResult &) = delete;

        AggregationResult(AggregationResult &&) = default;

        AggregationResult &operator=(AggregationResult &&) = default;

        // Accessors for the results vector
        [[nodiscard]] size_t len() const;

        [[nodiscard]] const Column &get(size_t index) const;
    };

    // Direct exposure of cuDF's groupby aggregate() return type
    struct GroupByResult {
        Table keys;
        std::vector<AggregationResult> results;

        GroupByResult();

        ~GroupByResult();

        // Field accessors as methods
        [[nodiscard]] const Table &get_keys() const;

        [[nodiscard]] size_t results_size() const;

        [[nodiscard]] const AggregationResult &get_result(size_t index) const;

        AggregationResult &get_result_mut(size_t index);
    };

    // Table factory functions
    std::unique_ptr<Table> create_empty_table();

    std::unique_ptr<Table> create_table_from_columns(rust::Slice<Column * const> columns);

    // Parquet I/O
    std::unique_ptr<Table> read_parquet(rust::Str filename);

    void write_parquet(const Table &table, rust::Str filename);

    // Direct cuDF operations - 1:1 mappings
    std::unique_ptr<Table> apply_boolean_mask(const Table &table, const Column &boolean_mask);

    // Binary operations - direct cuDF mappings
    std::unique_ptr<Column> binary_operation_col_col(
        const ColumnView &lhs,
        const ColumnView &rhs,
        int32_t op,
        int32_t output_type_id);

    std::unique_ptr<Column> binary_operation_col_scalar(
        const ColumnView &lhs,
        const Scalar &rhs,
        int32_t op,
        int32_t output_type_id);

    std::unique_ptr<Column> binary_operation_scalar_col(
        const Scalar &lhs,
        const ColumnView &rhs,
        int32_t op,
        int32_t output_type_id);

    // Aggregation factory functions - direct cuDF mappings (for reduce)
    std::unique_ptr<Aggregation> make_sum_aggregation();

    std::unique_ptr<Aggregation> make_min_aggregation();

    std::unique_ptr<Aggregation> make_max_aggregation();

    std::unique_ptr<Aggregation> make_mean_aggregation();

    std::unique_ptr<Aggregation> make_count_aggregation();

    // Aggregation factory functions - direct cuDF mappings (for groupby)
    std::unique_ptr<Aggregation> make_sum_aggregation_groupby();

    std::unique_ptr<Aggregation> make_min_aggregation_groupby();

    std::unique_ptr<Aggregation> make_max_aggregation_groupby();

    std::unique_ptr<Aggregation> make_mean_aggregation_groupby();

    std::unique_ptr<Aggregation> make_count_aggregation_groupby();

    // Reduction - direct cuDF mapping
    std::unique_ptr<Scalar> reduce(const Column &col, const Aggregation &agg, int32_t output_type_id);

    // GroupBy operations - direct cuDF mappings
    std::unique_ptr<GroupBy> groupby_create(const TableView &keys);

    std::unique_ptr<AggregationRequest> aggregation_request_create(const ColumnView &values);

    // Arrow interop - direct cuDF calls
    std::unique_ptr<Table> from_arrow_host(uint8_t *schema_ptr, uint8_t *device_array_ptr);

    void to_arrow_schema(const Table &table, uint8_t *out_schema_ptr);

    void to_arrow_host_array(const Table &table, uint8_t *out_array_ptr);

    // Utility functions
    rust::String get_cudf_version();
} // namespace libcudf_bridge
