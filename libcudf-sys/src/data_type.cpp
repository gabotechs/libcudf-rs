#include "data_type.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/types.hpp>

namespace libcudf_bridge {

    DataType::DataType(int32_t type_id)
        : inner(static_cast<cudf::type_id>(type_id)) {}

    DataType::DataType(int32_t type_id, int32_t scale)
        : inner(static_cast<cudf::type_id>(type_id), scale) {}

    int32_t DataType::id() const {
        return static_cast<int32_t>(inner.id());
    }

    int32_t DataType::scale() const {
        return inner.scale();
    }

    std::unique_ptr<DataType> new_data_type(int32_t type_id) {
        return std::make_unique<DataType>(type_id);
    }

    std::unique_ptr<DataType> new_data_type_with_scale(int32_t type_id, int32_t scale) {
        return std::make_unique<DataType>(type_id, scale);
    }

} // namespace libcudf_bridge
