#pragma once

#include <cstdint>
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

} // namespace libcudf_bridge
