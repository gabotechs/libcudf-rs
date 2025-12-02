#include "table.h"
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

    void TableView::to_arrow_schema(uint8_t *out_schema_ptr) const {
        std::vector<cudf::column_metadata> metadata(this->inner->num_columns());
        auto schema_unique = cudf::to_arrow_schema(*this->inner, cudf::host_span<cudf::column_metadata const>(metadata));
        auto *out_schema = reinterpret_cast<ArrowSchema *>(out_schema_ptr);
        *out_schema = *schema_unique.get();
        schema_unique.release();
    }

    void TableView::to_arrow_array(uint8_t *out_array_ptr) const {
        auto device_array_unique = cudf::to_arrow_host(*this->inner);
        auto *out_array = reinterpret_cast<ArrowArray *>(out_array_ptr);
        // Extract just the ArrowArray from the ArrowDeviceArray
        *out_array = device_array_unique->array;
        device_array_unique.release();
    }
} // namespace libcudf_bridge
