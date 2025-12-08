#include "table.h"
#include "groupby.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>

#include <nanoarrow/nanoarrow.h>
#include <nanoarrow/nanoarrow_device.h>

#include "cudf/interop.hpp"

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
        if (!inner) {
            throw std::runtime_error("Cannot get view of null table");
        }
        auto result = std::make_unique<TableView>();
        result->inner = std::make_unique<cudf::table_view>(inner->view());
        return result;
    }

    std::unique_ptr<ColumnVectorHelper> Table::release() const {
        if (!inner) {
            throw std::runtime_error("Cannot release columns from null table");
        }
        auto columns = inner->release();
        auto helper = std::make_unique<ColumnVectorHelper>();
        helper->columns.reserve(columns.size());
        for (auto &cudf_col : columns) {
            auto col = column_from_unique_ptr(std::move(cudf_col));
            helper->columns.push_back(std::move(col));
        }
        return helper;
    }

    // TableView implementation
    TableView::TableView() : inner(nullptr) {
    }

    TableView::~TableView() = default;

    size_t TableView::num_columns() const {
        if (!inner) {
            return 0;
        }
        return inner->num_columns();
    }

    size_t TableView::num_rows() const {
        if (!inner) {
            return 0;
        }
        return inner->num_rows();
    }

    std::unique_ptr<TableView> TableView::select(const rust::Slice<const int32_t> column_indices) const {
        if (!inner) {
            throw std::runtime_error("Cannot select from null table view");
        }
        std::vector<cudf::size_type> indices;
        indices.reserve(column_indices.size());
        for (const auto idx: column_indices) {
            if (idx < 0 || idx >= inner->num_columns()) {
                throw std::out_of_range("Column index out of bounds in select");
            }
            indices.emplace_back(idx);
        }

        auto result = std::make_unique<TableView>();
        result->inner = std::make_unique<cudf::table_view>(inner->select(indices));
        return result;
    }

    std::unique_ptr<ColumnView> TableView::column(const int32_t index) const {
        if (!inner) {
            throw std::runtime_error("Cannot get column from null table view");
        }
        if (index < 0 || index >= inner->num_columns()) {
            throw std::out_of_range("Column index out of bounds");
        }
        auto result = std::make_unique<ColumnView>();
        result->inner = std::make_unique<cudf::column_view>(inner->column(index));
        return result;
    }

    void TableView::to_arrow_schema(uint8_t *out_schema_ptr) const {
        if (!inner) {
            throw std::runtime_error("Cannot convert null table view to arrow schema");
        }
        std::vector<cudf::column_metadata> metadata(this->inner->num_columns());
        auto schema_unique =
                cudf::to_arrow_schema(*this->inner, cudf::host_span<cudf::column_metadata const>(metadata));
        auto *out_schema = reinterpret_cast<ArrowSchema *>(out_schema_ptr);
        *out_schema = *schema_unique.get();
        schema_unique.release();
    }

    void TableView::to_arrow_array(uint8_t *out_array_ptr) const {
        if (!inner) {
            throw std::runtime_error("Cannot convert null table view to arrow array");
        }
        auto device_array_unique = cudf::to_arrow_host(*this->inner);
        auto *out_array = reinterpret_cast<ArrowArray *>(out_array_ptr);
        // Extract just the ArrowArray from the ArrowDeviceArray
        *out_array = device_array_unique->array;
        device_array_unique.release();
    }

    [[nodiscard]] std::unique_ptr<TableView> TableView::clone() const {
        auto cloned = std::make_unique<TableView>();
        if (inner) {
            cloned->inner = std::make_unique<cudf::table_view>(*inner);
        }
        return cloned;
    }

    // TableView factory function
    std::unique_ptr<TableView> create_table_view_from_column_views(rust::Slice<const ColumnView *const> column_views) {
        std::vector<cudf::column_view> views;
        views.reserve(column_views.size());

        for (const auto *col_view_ptr: column_views) {
            views.push_back(*col_view_ptr->inner);
        }

        auto result = std::make_unique<TableView>();
        result->inner = std::make_unique<cudf::table_view>(views);
        return result;
    }
} // namespace libcudf_bridge
