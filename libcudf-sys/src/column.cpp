#include "column.h"
#include "data_type.h"
#include "scalar.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cinttypes>
#include <cstdint>
#include <cudf/column/column.hpp>
#include <cudf/copying.hpp>
#include <cudf/unary.hpp>

#include <memory>


namespace libcudf_bridge {
    // ColumnView implementation
    ColumnView::ColumnView() : inner(nullptr) {
    }

    ColumnView::~ColumnView() = default;

    // Get number of elements
    int32_t ColumnView::size() const {
        if (!inner) {
            throw std::runtime_error("Cannot get size of null column view");
        }
        return inner->size();
    }

    // Get the raw device pointer to the column view's data
    [[nodiscard]] uintptr_t ColumnView::head() const {
        if (!inner) {
            throw std::runtime_error("Cannot get data pointer of null column view");
        }
        return reinterpret_cast<uintptr_t>(inner->head());
    }

    // Get the data type of the column view
    [[nodiscard]] std::unique_ptr<DataType> ColumnView::type() const {
        if (!inner) {
            throw std::runtime_error("Cannot get data type of null column view");
        }
        return std::make_unique<DataType>(inner->type());
    }

    [[nodiscard]] std::unique_ptr<ColumnView> column_view_clone(const ColumnView& view) {
        if (!view.inner) {
            throw std::runtime_error("Cannot clone null column view");
        }
        auto cloned = std::make_unique<ColumnView>();
        cloned->inner = std::make_unique<cudf::column_view>(*view.inner);
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
        if (!inner) {
            throw std::runtime_error("Cannot get null count of null column view");
        }
        return inner->null_count();
    }

    [[nodiscard]] bool ColumnView::nullable() const {
        if (!inner) {
            throw std::runtime_error("Cannot inspect nullability of null column view");
        }
        return inner->nullable();
    }

    [[nodiscard]] int32_t ColumnView::num_children() const {
        if (!inner) {
            throw std::runtime_error("Cannot inspect children of null column view");
        }
        return inner->num_children();
    }

    [[nodiscard]] std::unique_ptr<ColumnView> ColumnView::child(const int32_t index) const {
        if (!inner) {
            throw std::runtime_error("Cannot get child of null column view");
        }
        auto result = std::make_unique<ColumnView>();
        result->inner = std::make_unique<cudf::column_view>(inner->child(index));
        return result;
    }

    [[nodiscard]] uintptr_t ColumnView::null_mask() const {
        if (!inner) {
            throw std::runtime_error("Cannot get null mask of null column view");
        }
        return reinterpret_cast<uintptr_t>(inner->null_mask());
    }

    // Column implementation
    Column::Column() : inner(nullptr) {
    }

    Column::~Column() = default;

    // Get number of elements
    int32_t Column::size() const {
        if (!inner) {
            throw std::runtime_error("Cannot get size of null column");
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
    [[nodiscard]] std::unique_ptr<DataType> Column::type() const {
        if (!inner) {
            throw std::runtime_error("Cannot get data type of null column");
        }
        return std::make_unique<DataType>(inner->type());
    }

    size_t Column::alloc_size() const {
        if (!inner) {
            throw std::runtime_error("Cannot get allocation size of null column");
        }
        return inner->alloc_size();
    }

    // Helper function to create Column from unique_ptr<cudf::column>
    Column column_from_unique_ptr(std::unique_ptr<cudf::column> col) {
        Column c;
        c.inner = std::move(col);
        return c;
    }

    std::unique_ptr<ColumnView> column_view_create(
        const DataType& type,
        int32_t size,
        uintptr_t data,
        uintptr_t null_mask,
        int32_t null_count,
        int32_t offset,
        rust::Slice<const ColumnView *const> children) {
        std::vector<cudf::column_view> child_views;
        child_views.reserve(children.size());
        for (const auto* child : children) {
            if (child == nullptr || !child->inner) {
                throw std::invalid_argument("Cannot construct a column view with a null child");
            }
            child_views.push_back(*child->inner);
        }

        auto result = std::make_unique<ColumnView>();
        result->inner = std::make_unique<cudf::column_view>(
            type.inner,
            size,
            reinterpret_cast<void const*>(data),
            reinterpret_cast<cudf::bitmask_type const*>(null_mask),
            null_count,
            offset,
            child_views);
        return result;
    }

    // Cast a column to a different data type
    std::unique_ptr<Column> cast(
        const ColumnView &input,
        const DataType &target_type,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr) {
        if (!input.inner) {
            throw std::runtime_error("Cannot cast null column view");
        }
        auto result = std::make_unique<Column>();
        result->inner = cudf::cast(*input.inner, target_type.inner, stream.inner, mr.inner);
        return result;
    }

    // Extract a scalar from a column at the specified index
    std::unique_ptr<Scalar> get_element(
        const ColumnView &column,
        int32_t index,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr) {
        if (!column.inner) {
            throw std::runtime_error("Cannot get element from null column view");
        }
        auto result = std::make_unique<Scalar>();
        result->inner = cudf::get_element(
            *column.inner,
            index,
            stream.inner,
            mr.inner);
        return result;
    }
} // namespace libcudf_bridge
