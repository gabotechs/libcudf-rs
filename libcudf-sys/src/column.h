#pragma once

#include <memory>
#include "rust/cxx.h"

// Forward declarations of cuDF types
namespace cudf {
    class column;
    class column_view;
    class scalar;
}

namespace libcudf_bridge {
    // Opaque wrapper for cuDF column_view
    struct ColumnView {
        std::unique_ptr<cudf::column_view> inner;

        ColumnView();
        ~ColumnView();

        // Get number of elements
        [[nodiscard]] size_t size() const;
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
    };

    // Helper function to create Column from unique_ptr<cudf::column>
    Column column_from_unique_ptr(std::unique_ptr<cudf::column> col);

    // Opaque wrapper for cuDF scalar
    struct Scalar {
        std::unique_ptr<cudf::scalar> inner;

        Scalar();
        ~Scalar();

        // Check if the scalar is valid (not null)
        [[nodiscard]] bool is_valid() const;
    };
} // namespace libcudf_bridge
