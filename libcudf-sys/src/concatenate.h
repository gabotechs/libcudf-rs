#pragma once

#include <memory>

#include "column.h"
#include "memory_resource.h"
#include "rust/cxx.h"
#include "stream.h"
#include "table.h"

namespace libcudf_bridge {
    [[nodiscard]] std::unique_ptr<Table> concatenate(
        rust::Slice<const std::unique_ptr<TableView>> views,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr);

    [[nodiscard]] std::unique_ptr<Column> concatenate(
        rust::Slice<const std::unique_ptr<ColumnView>> views,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr);
} // namespace libcudf_bridge
