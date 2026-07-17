#pragma once

#include <memory>
#include <vector>
#include "rust/cxx.h"
#include "column.h"
#include "stream.h"
#include "memory_resource.h"
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>

namespace libcudf_bridge {
    // Forward declaration
    struct ColumnVectorHelper;

    // cxx cannot return `cudf::column const&` directly. This mechanical wrapper
    // must not outlive the Table that owns the referenced column.
    struct ColumnRef {
        const cudf::column* inner;

        explicit ColumnRef(const cudf::column& column);

        [[nodiscard]] size_t alloc_size() const;
    };

    // Opaque wrapper for cuDF table_view
    struct TableView {
        std::unique_ptr<cudf::table_view> inner;

        TableView();

        ~TableView();

        // Get number of columns
        [[nodiscard]] int32_t num_columns() const;

        // Get number of rows
        [[nodiscard]] int32_t num_rows() const;

        // Select specific columns by indices
        [[nodiscard]] std::unique_ptr<TableView> select(rust::Slice<const int32_t> column_indices) const;

        // Get column view at index
        [[nodiscard]] std::unique_ptr<ColumnView> column(int32_t index) const;

    };

    // Opaque wrapper for cuDF table
    struct Table {
        std::unique_ptr<cudf::table> inner;

        Table();

        ~Table();

        // Get number of columns
        [[nodiscard]] int32_t num_columns() const;

        // Get number of rows
        [[nodiscard]] int32_t num_rows() const;

        // Get a view of this table
        [[nodiscard]] std::unique_ptr<TableView> view() const;

        // Get a const column reference at index
        [[nodiscard]] std::unique_ptr<ColumnRef> get_column(int32_t index) const;

        [[nodiscard]] std::unique_ptr<ColumnVectorHelper> release();
    };

    // Table factory functions
    [[nodiscard]] std::unique_ptr<Table> create_empty_table();

    [[nodiscard]] std::unique_ptr<Table> create_table_from_columns_move(rust::Slice<Column *const> columns);

    // TableView factory functions
    [[nodiscard]] std::unique_ptr<TableView> table_view_clone(const TableView& view);

    [[nodiscard]] std::unique_ptr<TableView> create_table_view_from_column_views(rust::Slice<const ColumnView *const> column_views);
} // namespace libcudf_bridge
