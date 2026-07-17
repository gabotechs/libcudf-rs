#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <cudf/types.hpp>
#include "rust/cxx.h"

namespace libcudf_bridge {

    /// Wrapper for cuDF's data_type class
    struct DataType {
        /// Construct from a type_id
        explicit DataType(int32_t type_id);

        /// Construct from a type_id and scale (for fixed_point types)
        DataType(int32_t type_id, int32_t scale);

        /// Wrap an upstream data_type value.
        explicit DataType(cudf::data_type type);

        /// Get the type_id
        [[nodiscard]] int32_t id() const;

        /// Get the scale (for fixed_point types)
        [[nodiscard]] int32_t scale() const;

        /// Internal cuDF data_type
        cudf::data_type inner;
    };

    /// Create a DataType from a type_id
    [[nodiscard]] std::unique_ptr<DataType> new_data_type(int32_t type_id);

    /// Create a DataType from a type_id and scale (for decimals)
    [[nodiscard]] std::unique_ptr<DataType> new_data_type_with_scale(int32_t type_id, int32_t scale);

    /// Return whether `type` has a fixed-width representation.
    [[nodiscard]] bool is_fixed_width(const DataType& type);

    /// Return the byte width of a fixed-width `type`.
    [[nodiscard]] std::size_t size_of(const DataType& type);

    /// Return the padded allocation size for a null mask.
    [[nodiscard]] std::size_t bitmask_allocation_size_bytes(
        std::int32_t number_of_bits,
        std::size_t padding_boundary);

} // namespace libcudf_bridge
