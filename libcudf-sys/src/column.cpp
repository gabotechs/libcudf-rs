#include "column.h"
#include "data_type.h"
#include "scalar.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/column/column.hpp>
#include <cudf/interop.hpp>
#include <cudf/copying.hpp>

#include <nanoarrow/nanoarrow.h>
#include <nanoarrow/nanoarrow_device.h>


namespace libcudf_bridge {
    // ColumnView implementation
    ColumnView::ColumnView() : inner(nullptr) {
    }

    ColumnView::~ColumnView() = default;

    size_t ColumnView::size() const {
        if (!inner) {
            return 0;
        }
        return inner->size();
    }

    void ColumnView::to_arrow_array(uint8_t *out_array_ptr) const {
        if (!inner) {
            throw std::runtime_error("Cannot convert null column view to arrow array");
        }
        auto device_array_unique = cudf::to_arrow_host(*this->inner);
        auto *out_array = reinterpret_cast<ArrowDeviceArray *>(out_array_ptr);
        *out_array = *device_array_unique.get();
        device_array_unique.release();
    }

    [[nodiscard]] uint64_t ColumnView::data_ptr() const {
        if (!inner) {
            return 0;
        }
        return reinterpret_cast<uint64_t>(inner->head());
    }

    [[nodiscard]] std::unique_ptr<DataType> ColumnView::data_type() const {
        if (!inner) {
            throw std::runtime_error("Cannot get data type of null column view");
        }
        auto dtype = inner->type();
        auto type_id = static_cast<int32_t>(dtype.id());

        // Only pass scale for decimal types
        if (dtype.id() == cudf::type_id::DECIMAL32 ||
            dtype.id() == cudf::type_id::DECIMAL64 ||
            dtype.id() == cudf::type_id::DECIMAL128) {
            return std::make_unique<DataType>(type_id, dtype.scale());
        }
        return std::make_unique<DataType>(type_id);
    }

    [[nodiscard]] std::unique_ptr<ColumnView> ColumnView::clone() const {
        auto cloned = std::make_unique<ColumnView>();
        if (inner) {
            cloned->inner = std::make_unique<cudf::column_view>(*inner);
        }
        return cloned;
    }

    [[nodiscard]] std::unique_ptr<ColumnView> ColumnView::slice(size_t offset, size_t length) const {
        if (!inner) {
            throw std::runtime_error("Cannot slice null column view");
        }
        if (offset + length > static_cast<size_t>(inner->size())) {
            throw std::out_of_range("Slice bounds out of range");
        }

        // Use cuDF's native slice function from cudf/copying.hpp
        // slice() takes pairs of [start, end) indices and returns a vector of views
        auto start = static_cast<cudf::size_type>(offset);
        auto end = static_cast<cudf::size_type>(offset + length);
        std::vector indices = {start, end};

        auto sliced_views = cudf::slice(*inner, indices);

        // We expect exactly one view back since we provided one [start, end) pair
        if (sliced_views.empty()) {
            throw std::runtime_error("cudf::slice returned no views");
        }

        auto result = std::make_unique<ColumnView>();
        result->inner = std::make_unique<cudf::column_view>(sliced_views.at(0));
        return result;
    }

    // Gets the current offset in case this column is a slice of another one
    int32_t ColumnView::offset() const {
        if (!inner) {
            throw std::runtime_error("Cannot offset of null column view");
        }
        return inner->offset();
    }

    // Column implementation
    Column::Column() : inner(nullptr) {
    }

    Column::~Column() = default;

    size_t Column::size() const {
        if (!inner) {
            return 0;
        }
        return inner->size();
    }

    [[nodiscard]] std::unique_ptr<ColumnView> Column::view() const {
        if (!inner) {
            throw std::runtime_error("Cannot get view of null column");
        }
        auto result = std::make_unique<ColumnView>();
        result->inner = std::make_unique<cudf::column_view>(inner->view());
        return result;
    }

    [[nodiscard]] std::unique_ptr<DataType> Column::data_type() const {
        if (!inner) {
            throw std::runtime_error("Cannot get data type of null column");
        }
        auto dtype = inner->type();
        auto type_id = static_cast<int32_t>(dtype.id());

        // Only pass scale for decimal types
        if (dtype.id() == cudf::type_id::DECIMAL32 ||
            dtype.id() == cudf::type_id::DECIMAL64 ||
            dtype.id() == cudf::type_id::DECIMAL128) {
            return std::make_unique<DataType>(type_id, dtype.scale());
        }
        return std::make_unique<DataType>(type_id);
    }

    // Helper function to create Column from unique_ptr
    Column column_from_unique_ptr(std::unique_ptr<cudf::column> col) {
        Column c;
        c.inner = std::move(col);
        return c;
    }

    // Extract a scalar from a column at the specified index
    std::unique_ptr<Scalar> get_element(const ColumnView &column, size_t index) {
        if (!column.inner) {
            throw std::runtime_error("Cannot get element from null column view");
        }
        if (index >= static_cast<size_t>(column.inner->size())) {
            throw std::out_of_range("Index out of bounds for get_element");
        }
        auto result = std::make_unique<Scalar>();
        result->inner = cudf::get_element(*column.inner, static_cast<cudf::size_type>(index));
        return result;
    }
} // namespace libcudf_bridge
