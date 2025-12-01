#include "bridge.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>

namespace libcudf_bridge {
    // Table implementation
    Table::Table() : inner(nullptr) {
    }

    Table::~Table() = default;

    size_t Table::num_columns() const {
        if (!inner) {
            return 0;
        }
        return inner->num_columns();
    }

    size_t Table::num_rows() const {
        if (!inner) {
            return 0;
        }
        return inner->num_rows();
    }

    std::unique_ptr<TableView> Table::view() const {
        auto result = std::make_unique<TableView>();
        result->inner = std::make_unique<cudf::table_view>(inner->view());
        return result;
    }

    // TableView implementation
    TableView::TableView() : inner(nullptr) {
    }

    TableView::~TableView() = default;

    size_t TableView::num_columns() const {
        return inner->num_columns();
    }

    size_t TableView::num_rows() const {
        return inner->num_rows();
    }

    std::unique_ptr<TableView> TableView::select(const rust::Slice<const int32_t> column_indices) const {
        std::vector<cudf::size_type> indices;
        indices.reserve(column_indices.size());
        for (const auto idx: column_indices) {
            indices.push_back(idx);
        }

        auto result = std::make_unique<TableView>();
        result->inner = std::make_unique<cudf::table_view>(inner->select(indices));
        return result;
    }

    std::unique_ptr<ColumnView> TableView::column(const int32_t index) const {
        auto result = std::make_unique<ColumnView>();
        result->inner = std::make_unique<cudf::column_view>(inner->column(index));
        return result;
    }
} // namespace libcudf_bridge
