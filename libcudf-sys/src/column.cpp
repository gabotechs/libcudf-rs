#include "column.h"
#include "cudf/null_mask.hpp"
#include "cudf/types.hpp"
#include <cuda_runtime_api.h>
#include "data_type.h"
#include "scalar.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cinttypes>
#include <cstdint>
#include <cudf/column/column.hpp>
#include <cudf/interop.hpp>
#include <cudf/copying.hpp>
#include <cudf/unary.hpp>

#include <memory>
#include <nanoarrow/nanoarrow.h>
#include <nanoarrow/nanoarrow_device.h>


namespace libcudf_bridge {
    // ColumnView implementation
    ColumnView::ColumnView() : inner(nullptr) {
    }

    ColumnView::~ColumnView() = default;

    // Get number of elements
    size_t ColumnView::size() const {
        if (!inner) {
            return 0;
        }
        return inner->size();
    }

    // Get the column's data as an FFI Arrow Array
    void ColumnView::to_arrow_array(uint8_t *out_array_ptr) const {
        if (!inner) {
            throw std::runtime_error("Cannot convert null column view to arrow array");
        }
        auto device_array_unique = cudf::to_arrow_host(*this->inner);
        auto *out_array = reinterpret_cast<ArrowDeviceArray*>(out_array_ptr);
        *out_array = *device_array_unique.get();
        device_array_unique.release();
    }

    // Get the raw device pointer to the column view's data
    [[nodiscard]] uint64_t ColumnView::data_ptr() const {
        if (!inner) {
            return 0;
        }
        return reinterpret_cast<uint64_t>(inner->head());
    }

    // Get the data type of the column view
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

    // Clone this column view
    [[nodiscard]] std::unique_ptr<ColumnView> ColumnView::clone() const {
        auto cloned = std::make_unique<ColumnView>();
        if (inner) {
            cloned->inner = std::make_unique<cudf::column_view>(*inner);
        }
        return cloned;
    }

    // Get the offset of the current ColumnView in case it was a slice of another one
    int32_t ColumnView::offset() const {
        if (!inner) {
            throw std::runtime_error("Cannot offset of null column view");
        }
        return inner->offset();
    }

    // Returns how many nulls this column has
    [[nodiscard]] int32_t ColumnView::null_count() const {
        if (inner) {
            return inner->null_count();
        }
        return 0;
    }

    // Helper functions for memory size calculations
    size_t calculate_buffer_memory_size(const cudf::column_view& view) {
        // Calculate size based on data type
        size_t data_size = cudf::size_of(view.type()) * view.size();

        // For strings add offset buffer size
        if (view.type().id() == cudf::type_id::STRING) {
            data_size += (view.size() + 1) * sizeof(int32_t);
        }

        // For nested types recursively add child buffer sizes
        for (cudf::size_type i = 0; i < view.num_children(); ++i) {
            data_size += calculate_buffer_memory_size(view.child(i));
        }

        return data_size;
    }

    size_t calculate_null_mask_size(const cudf::column_view& view) {
        size_t size = 0;

        if (view.nullable()) {
            size += cudf::bitmask_allocation_size_bytes(view.size());
        }

        for (cudf::size_type i = 0; i < view.num_children(); ++i) {
            size += calculate_null_mask_size(view.child(i));
        }

        return size;
    }

    size_t calculate_array_memory_size(const cudf::column_view& view) {
        return calculate_buffer_memory_size(view) + calculate_null_mask_size(view);
    }

    // Get buffer memory size (data + offsets, no null mask)
    [[nodiscard]] size_t ColumnView::get_buffer_memory_size() const {
        if (!inner) {
            return 0;
        }
        return calculate_buffer_memory_size(*inner);
    }

    // Get total array memory size (data + offsets + null mask + children)
    [[nodiscard]] size_t ColumnView::get_array_memory_size() const {
        if (!inner) {
            return 0;
        }
        return calculate_array_memory_size(*inner);
    }

    // Transfer the null buffer
    [[nodiscard]] rust::Vec<uint8_t> ColumnView::get_null_buffer() const {
        if (!inner || inner->null_count() == 0) {
            return rust::Vec<uint8_t>();
        }

        const auto& view = *inner;

        // Calculate null mask size
        size_t mask_size = cudf::bitmask_allocation_size_bytes(view.size());

        rust::Vec<uint8_t> host_buffer;
        host_buffer.reserve(mask_size);

        // Resize to actual size so cudaMemcpy has a valid destination
        for (size_t i = 0; i < mask_size; ++i) {
            host_buffer.push_back(0);
        }

        cudaError_t err = cudaMemcpy(
            host_buffer.data(),
            view.null_mask(),
            mask_size,
            cudaMemcpyDeviceToHost
        );

        if (err != cudaSuccess) {
            throw std::runtime_error(std::string("cudaMemcpy failed: ") + cudaGetErrorString(err));
        }

        return host_buffer;
    }

    // Column implementation
    Column::Column() : inner(nullptr) {
    }

    Column::~Column() = default;

    // Get number of elements
    size_t Column::size() const {
        if (!inner) {
            return 0;
        }
        return inner->size();
    }

    // Get the column as a read-only view
    [[nodiscard]] std::unique_ptr<ColumnView> Column::view() const {
        if (!inner) {
            throw std::runtime_error("Cannot get view of null column");
        }
        auto result = std::make_unique<ColumnView>();
        result->inner = std::make_unique<cudf::column_view>(inner->view());
        return result;
    }

    // Get the data type of the column
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

    // Helper function to create Column from unique_ptr<cudf::column>
    Column column_from_unique_ptr(std::unique_ptr<cudf::column> col) {
        Column c;
        c.inner = std::move(col);
        return c;
    }

    // Cast a column to a different data type
    std::unique_ptr<Column> cast_column(const ColumnView &input, const DataType &target_type) {
        if (!input.inner) {
            throw std::runtime_error("Cannot cast null column view");
        }
        auto result = std::make_unique<Column>();
        result->inner = cudf::cast(*input.inner, target_type.inner);
        return result;
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
