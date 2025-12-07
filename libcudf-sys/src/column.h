#pragma once

#include <memory>

#include "cudf/types.hpp"
#include "rust/cxx.h"

// Forward declarations of cuDF types
namespace cudf {
    class column;
    class column_view;
    class scalar;
}

namespace libcudf_bridge {
    struct DataType;

    // Opaque wrapper for cuDF column_view
    struct ColumnView {
        std::unique_ptr<cudf::column_view> inner;

        ColumnView();

        ~ColumnView();

        // Get number of elements
        [[nodiscard]] size_t size() const;

        // Get the column's data as an FFI Arrow Array
        void to_arrow_array(uint8_t *out_array_ptr) const;

        // Get the raw device pointer to the column view's data
        [[nodiscard]] uint64_t data_ptr() const;

        // Get the data type of the column view
        [[nodiscard]] std::unique_ptr<DataType> data_type() const;

        // Clone this column view
        [[nodiscard]] std::unique_ptr<ColumnView> clone() const;

        // Create a sliced view of this column
        [[nodiscard]] std::unique_ptr<ColumnView> slice(size_t offset, size_t length) const;

        // Create a sliced view of this column
        [[nodiscard]] int32_t offset() const;
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
        [[nodiscard]] size_t size() const;

        // Get the column as a read-only view
        [[nodiscard]] std::unique_ptr<ColumnView> view() const;

        // Get the data type of the column
        [[nodiscard]] std::unique_ptr<DataType> data_type() const;
    };

    // Forward declaration
    struct Scalar;

    // Helper function to create Column from unique_ptr<cudf::column>
    Column column_from_unique_ptr(std::unique_ptr<cudf::column> col);

    // Extract a scalar from a column at the specified index
    std::unique_ptr<Scalar> get_element(const ColumnView &column, size_t index);
} // namespace libcudf_bridge
