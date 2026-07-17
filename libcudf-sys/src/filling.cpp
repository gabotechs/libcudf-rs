#include "filling.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/column/column_factories.hpp>
#include <cudf/filling.hpp>

#include <stdexcept>

namespace libcudf_bridge {
    namespace {
        const cudf::scalar& require_scalar(const Scalar& scalar) {
            if (!scalar.inner) {
                throw std::invalid_argument("Cannot use a null scalar for filling");
            }
            return *scalar.inner;
        }
    } // namespace

    std::unique_ptr<Column> make_column_from_scalar(
        const Scalar& scalar,
        int32_t size,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr) {
        auto result = std::make_unique<Column>();
        result->inner = cudf::make_column_from_scalar(
            require_scalar(scalar), size, stream.inner, mr.inner);
        return result;
    }

    std::unique_ptr<Column> sequence(
        int32_t size,
        const Scalar& init,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr) {
        auto result = std::make_unique<Column>();
        result->inner = cudf::sequence(size, require_scalar(init), stream.inner, mr.inner);
        return result;
    }

    std::unique_ptr<Column> sequence(
        int32_t size,
        const Scalar& init,
        const Scalar& step,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr) {
        auto result = std::make_unique<Column>();
        result->inner = cudf::sequence(
            size, require_scalar(init), require_scalar(step), stream.inner, mr.inner);
        return result;
    }
} // namespace libcudf_bridge
