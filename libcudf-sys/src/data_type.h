#pragma once

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

        /// Get the type_id
        int32_t id() const;

        /// Get the scale (for fixed_point types)
        int32_t scale() const;

        /// Internal cuDF data_type
        cudf::data_type inner;
    };

    /// Create a DataType from a type_id
    std::unique_ptr<DataType> new_data_type(int32_t type_id);

    /// Create a DataType from a type_id and scale (for decimals)
    std::unique_ptr<DataType> new_data_type_with_scale(int32_t type_id, int32_t scale);

} // namespace libcudf_bridge
