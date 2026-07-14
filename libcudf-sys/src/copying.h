#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "column.h"
#include "memory_resource.h"
#include "rust/cxx.h"
#include "scalar.h"
#include "stream.h"
#include "table.h"

namespace libcudf_bridge {
    [[nodiscard]] std::unique_ptr<Table> gather(
        const TableView& source_table,
        const ColumnView& gather_map,
        int32_t out_of_bounds_policy,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr);

    [[nodiscard]] std::unique_ptr<Table> scatter(
        const TableView& source,
        const ColumnView& indices,
        const TableView& target,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr);

    [[nodiscard]] std::unique_ptr<Table> scatter(
        rust::Slice<const Scalar *const> source,
        const ColumnView& indices,
        const TableView& target,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr);

    struct ColumnViews {
        std::vector<cudf::column_view> inner;

        explicit ColumnViews(std::vector<cudf::column_view> views);
        ~ColumnViews();

        [[nodiscard]] size_t len() const;
        [[nodiscard]] bool is_empty() const;
        [[nodiscard]] std::unique_ptr<ColumnView> get(size_t index) const;
    };

    struct TableViews {
        std::vector<cudf::table_view> inner;

        explicit TableViews(std::vector<cudf::table_view> views);
        ~TableViews();

        [[nodiscard]] size_t len() const;
        [[nodiscard]] bool is_empty() const;
        [[nodiscard]] std::unique_ptr<TableView> get(size_t index) const;
    };

    [[nodiscard]] std::unique_ptr<ColumnViews> slice(
        const ColumnView& column,
        rust::Slice<const int32_t> indices,
        const CudaStreamView& stream);

    [[nodiscard]] std::unique_ptr<TableViews> slice(
        const TableView& table,
        rust::Slice<const int32_t> indices,
        const CudaStreamView& stream);
} // namespace libcudf_bridge
