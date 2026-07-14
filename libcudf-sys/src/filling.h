#pragma once

#include <cstdint>
#include <memory>

#include "column.h"
#include "memory_resource.h"
#include "scalar.h"
#include "stream.h"

namespace libcudf_bridge {
    [[nodiscard]] std::unique_ptr<Column> make_column_from_scalar(
        const Scalar& scalar,
        int32_t size,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr);

    [[nodiscard]] std::unique_ptr<Column> sequence(
        int32_t size,
        const Scalar& init,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr);

    [[nodiscard]] std::unique_ptr<Column> sequence(
        int32_t size,
        const Scalar& init,
        const Scalar& step,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr);
} // namespace libcudf_bridge
