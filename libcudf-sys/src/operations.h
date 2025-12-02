#pragma once

#include <memory>
#include <cstdint>
#include "rust/cxx.h"
#include "table.h"
#include "column.h"

// Forward declarations of Arrow C ABI types
struct ArrowSchema;
struct ArrowDeviceArray;

namespace libcudf_bridge {
    // Direct cuDF operations - 1:1 mappings
    std::unique_ptr<Table> apply_boolean_mask(const Table &table, const Column &boolean_mask);

    // Arrow interop - direct cuDF calls
    std::unique_ptr<Table> table_from_arrow_host(uint8_t const *schema_ptr, uint8_t const *device_array_ptr);

    std::unique_ptr<Table> concat_table_views(rust::Slice<const std::unique_ptr<TableView>> views);
    std::unique_ptr<Column> concat_column_views(rust::Slice<const std::unique_ptr<ColumnView>> views);

    // Utility functions
    rust::String get_cudf_version();
} // namespace libcudf_bridge
