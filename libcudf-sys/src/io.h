#pragma once

#include <memory>
#include "rust/cxx.h"
#include "table.h"

namespace libcudf_bridge {

    // Parquet I/O
    std::unique_ptr<Table> read_parquet(rust::Str filename);
    void write_parquet(const Table &table, rust::Str filename);
} // namespace libcudf_bridge
