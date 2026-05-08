#pragma once

#include <memory>
#include <cstdint>
#include "rust/cxx.h"
#include "table.h"
#include "column.h"
#include "stream.h"
#include "memory_resource.h"

namespace libcudf_bridge {

    // Sorting operations - direct cuDF mappings
    std::unique_ptr<Table> sort_table(
        const TableView &input,
        rust::Slice<const int32_t> column_order,
        rust::Slice<const int32_t> null_precedence,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr);

    std::unique_ptr<Table> stable_sort_table(
        const TableView &input,
        rust::Slice<const int32_t> column_order,
        rust::Slice<const int32_t> null_precedence,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr);

    std::unique_ptr<Column> sorted_order(
        const TableView &input,
        rust::Slice<const int32_t> column_order,
        rust::Slice<const int32_t> null_precedence,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr);

    std::unique_ptr<Column> stable_sorted_order(
        const TableView &input,
        rust::Slice<const int32_t> column_order,
        rust::Slice<const int32_t> null_precedence,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr);

    bool is_sorted(
        const TableView &input,
        rust::Slice<const int32_t> column_order,
        rust::Slice<const int32_t> null_precedence,
        const CudaStreamView &stream);

    std::unique_ptr<Table> sort_by_key(
        const TableView &values,
        const TableView &keys,
        rust::Slice<const int32_t> column_order,
        rust::Slice<const int32_t> null_precedence,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr);

    std::unique_ptr<Table> stable_sort_by_key(
        const TableView &values,
        const TableView &keys,
        rust::Slice<const int32_t> column_order,
        rust::Slice<const int32_t> null_precedence,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr);
} // namespace libcudf_bridge
