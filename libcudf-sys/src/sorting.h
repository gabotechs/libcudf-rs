#pragma once

#include <memory>
#include <cstdint>
#include "rust/cxx.h"
#include "table.h"
#include "column.h"

namespace libcudf_bridge {

    // Sorting operations - direct cuDF mappings
    std::unique_ptr<Table> sort_table(
        const TableView &input,
        rust::Slice<const int32_t> column_order,
        rust::Slice<const int32_t> null_precedence);

    std::unique_ptr<Table> stable_sort_table(
        const TableView &input,
        rust::Slice<const int32_t> column_order,
        rust::Slice<const int32_t> null_precedence);

    std::unique_ptr<Column> sorted_order(
        const TableView &input,
        rust::Slice<const int32_t> column_order,
        rust::Slice<const int32_t> null_precedence);

    bool is_sorted(
        const TableView &input,
        rust::Slice<const int32_t> column_order,
        rust::Slice<const int32_t> null_precedence);

    std::unique_ptr<Table> sort_by_key(
        const TableView &values,
        const TableView &keys,
        rust::Slice<const int32_t> column_order,
        rust::Slice<const int32_t> null_precedence);

    std::unique_ptr<Table> stable_sort_by_key(
        const TableView &values,
        const TableView &keys,
        rust::Slice<const int32_t> column_order,
        rust::Slice<const int32_t> null_precedence);
} // namespace libcudf_bridge
