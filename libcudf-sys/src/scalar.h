#pragma once

#include <memory>
#include <cstdint>

#include "rust/cxx.h"

// Forward declarations
namespace cudf {
    class scalar;
}

namespace libcudf_bridge {
    struct DataType;

    // Opaque wrapper for cuDF scalar
    struct Scalar {
        std::unique_ptr<cudf::scalar> inner;

        Scalar();

        ~Scalar();

        // Check if the scalar is valid (not null)
        [[nodiscard]] bool is_valid() const;

        // Get the data type of the scalar
        [[nodiscard]] std::unique_ptr<DataType> data_type() const;

        // Clone this scalar (deep copy)
        [[nodiscard]] std::unique_ptr<Scalar> clone() const;
    };
} // namespace libcudf_bridge
