#pragma once

#include <memory>
#include "rust/cxx.h"
#include "table.h"
#include "stream.h"
#include "memory_resource.h"

namespace libcudf_bridge {

    // Parquet I/O
    std::unique_ptr<Table> read_parquet(
        rust::Str filename,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr);
    void write_parquet(const TableView &table, rust::Str filename, const CudaStreamView &stream);
} // namespace libcudf_bridge
