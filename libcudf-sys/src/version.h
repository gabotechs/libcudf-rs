#pragma once

#include "rust/cxx.h"

namespace libcudf_bridge {
    [[nodiscard]] rust::String get_cudf_version();
} // namespace libcudf_bridge
