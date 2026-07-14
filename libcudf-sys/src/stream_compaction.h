#pragma once

#include <cstdint>
#include <memory>

#include "column.h"
#include "memory_resource.h"
#include "rust/cxx.h"
#include "stream.h"
#include "table.h"

namespace libcudf_bridge {
    [[nodiscard]] std::unique_ptr<Table> apply_boolean_mask(
        const TableView& table,
        const ColumnView& boolean_mask,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr);

    [[nodiscard]] std::unique_ptr<Table> distinct(
        const TableView& input,
        rust::Slice<const int32_t> keys,
        int32_t keep,
        int32_t nulls_equal,
        int32_t nans_equal,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr);
} // namespace libcudf_bridge
