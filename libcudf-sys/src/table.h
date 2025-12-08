#pragma once

#include <memory>
#include <vector>
#include "rust/cxx.h"
#include "column.h"
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>

namespace libcudf_bridge {
    // Forward declaration
    struct ColumnVectorHelper;

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

        // Clone this table view
        [[nodiscard]] std::unique_ptr<TableView> clone() const;
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

        [[nodiscard]] std::unique_ptr<ColumnVectorHelper> release() const;
    };

    // Table factory functions
    std::unique_ptr<Table> create_empty_table();

    std::unique_ptr<Table> create_table_from_columns_move(rust::Slice<Column *const> columns);

    // TableView factory functions
    std::unique_ptr<TableView> create_table_view_from_column_views(rust::Slice<const ColumnView *const> column_views);
} // namespace libcudf_bridge
