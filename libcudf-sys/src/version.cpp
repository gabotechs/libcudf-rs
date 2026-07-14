#include "version.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/version_config.hpp>

#include <sstream>

namespace libcudf_bridge {
    rust::String get_cudf_version() {
        std::ostringstream version;
        version << CUDF_VERSION_MAJOR << "."
                << CUDF_VERSION_MINOR << "."
                << CUDF_VERSION_PATCH;
        return {version.str()};
    }
} // namespace libcudf_bridge
