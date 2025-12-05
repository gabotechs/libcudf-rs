#pragma once

#include <memory>
#include <vector>
#include "rust/cxx.h"
#include "column.h"

// Forward declarations of cuDF types
namespace cudf {
    class table;
    class table_view;
}

namespace libcudf_bridge {
    // Direct exposure of cuDF's aggregation_result
    struct ColumnCollection {
        std::vector<Column> results;

        ColumnCollection();
        ~ColumnCollection();

        // Delete copy, allow move
        ColumnCollection(const ColumnCollection &) = delete;
        ColumnCollection &operator=(const ColumnCollection &) = delete;
        ColumnCollection(ColumnCollection &&) = default;
        ColumnCollection &operator=(ColumnCollection &&) = default;

        // Accessors for the results vector
        [[nodiscard]] size_t len() const;
        [[nodiscard]] const Column &get(size_t index) const;
        [[nodiscard]] std::unique_ptr<Column> release(size_t index);
    };

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

        // Get the columns' data types as an FFI Arrow Schema
        void to_arrow_schema(uint8_t *out_schema_ptr) const;

        // Get the columns' data as an FFI Arrow Array
        void to_arrow_array(uint8_t *out_array_ptr) const;
    };

    // Opaque wrapper for cuDF table
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

        [[nodiscard]] std::unique_ptr<ColumnCollection> take() const;
    };

    // Table factory functions
    std::unique_ptr<Table> create_empty_table();
    std::unique_ptr<Table> create_table_from_columns_move(rust::Slice<Column *const> columns);

    // TableView factory functions
    std::unique_ptr<TableView> create_table_view_from_column_views(rust::Slice<const ColumnView *const> column_views);
} // namespace libcudf_bridge
