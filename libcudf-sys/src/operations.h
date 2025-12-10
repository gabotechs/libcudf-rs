#pragma once

#include <memory>
#include <cstdint>
#include "rust/cxx.h"
#include "table.h"
#include "column.h"

// Forward declarations of Arrow C ABI types
struct ArrowSchema;
struct ArrowDeviceArray;
struct ArrowArray;

namespace libcudf_bridge {
    // Direct cuDF operations - 1:1 mappings
    std::unique_ptr<Table> apply_boolean_mask(const TableView &table, const ColumnView &boolean_mask);

    // Arrow interop - direct cuDF calls
    std::unique_ptr<Table> table_from_arrow_host(uint8_t const *schema_ptr, uint8_t const *device_array_ptr);

    std::unique_ptr<Column> column_from_arrow(uint8_t const *schema_ptr, uint8_t const *array_ptr);

    std::unique_ptr<Table> concat_table_views(rust::Slice<const std::unique_ptr<TableView>> views);

    std::unique_ptr<Column> concat_column_views(rust::Slice<const std::unique_ptr<ColumnView>> views);

    // Gather rows from a table based on a gather map (column of indices)
    std::unique_ptr<Table> gather(const TableView &source_table, const ColumnView &gather_map);

    // Create a sliced view of a column
    std::unique_ptr<ColumnView> slice_column(const ColumnView &column, size_t offset, size_t length);

    // Utility functions
    rust::String get_cudf_version();
} // namespace libcudf_bridge
