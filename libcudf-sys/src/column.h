#pragma once

#include <memory>

#include "cudf/types.hpp"
#include "rust/cxx.h"
#include "stream.h"
#include "memory_resource.h"
#include <cudf/column/column.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/scalar/scalar.hpp>

namespace libcudf_bridge {
    struct DataType;

    // Opaque wrapper for cuDF column_view
    struct ColumnView {
        std::unique_ptr<cudf::column_view> inner;

        ColumnView();

        ~ColumnView();

        // Get number of elements
        [[nodiscard]] int32_t size() const;

        // Get the raw device pointer to the column view's data
        [[nodiscard]] uintptr_t head() const;

        // Get the data type of the column view
        [[nodiscard]] std::unique_ptr<DataType> type() const;

        // Get the offset of the current ColumnView in case it was a slice of another one
        [[nodiscard]] int32_t offset() const;

        // Returns how many nulls this column has
        [[nodiscard]] int32_t null_count() const;

        // Returns whether this view has a null mask
        [[nodiscard]] bool nullable() const;

        // Returns the number of child views
        [[nodiscard]] int32_t num_children() const;

        // Returns a child view by index
        [[nodiscard]] std::unique_ptr<ColumnView> child(int32_t index) const;

        // Return the null-mask device pointer encoded for cxx.
        [[nodiscard]] uintptr_t null_mask() const;
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
        [[nodiscard]] int32_t size() const;

        // Get the column as a read-only view
        [[nodiscard]] std::unique_ptr<ColumnView> view() const;

        // Get the data type of the column
        [[nodiscard]] std::unique_ptr<DataType> type() const;

        // Get the total device allocation size of the column
        [[nodiscard]] size_t alloc_size() const;
    };

    // Forward declaration
    struct Scalar;

    // Helper function to create Column from unique_ptr<cudf::column>
    [[nodiscard]] Column column_from_unique_ptr(std::unique_ptr<cudf::column> col);

    // Mechanical factory for cudf::column_view's copy constructor.
    [[nodiscard]] std::unique_ptr<ColumnView> column_view_clone(const ColumnView& view);

    [[nodiscard]] std::unique_ptr<ColumnView> column_view_create(
        const DataType& type,
        int32_t size,
        uintptr_t data,
        uintptr_t null_mask,
        int32_t null_count,
        int32_t offset,
        rust::Slice<const ColumnView *const> children);

    // Extract a scalar from a column at the specified index
    [[nodiscard]] std::unique_ptr<Scalar> get_element(
        const ColumnView &column,
        int32_t index,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr);

    // Cast a column to a different data type
    [[nodiscard]] std::unique_ptr<Column> cast(
        const ColumnView &input,
        const DataType &target_type,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr);

} // namespace libcudf_bridge
