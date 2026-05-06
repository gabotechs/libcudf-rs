#pragma once

#include <memory>
#include <cstdint>
#include "rust/cxx.h"
#include "table.h"
#include "column.h"
#include "scalar.h"
#include "stream.h"
#include "memory_resource.h"

// Forward declarations of Arrow C ABI types
struct ArrowSchema;
struct ArrowDeviceArray;
struct ArrowArray;

namespace libcudf_bridge {
    // Direct cuDF operations exposed through bridge-owned return types.
    std::unique_ptr<Table> apply_boolean_mask(const TableView &table, const ColumnView &boolean_mask);

    std::unique_ptr<Table> apply_boolean_mask_on(
        const TableView &table,
        const ColumnView &boolean_mask,
        const CudaStream &stream);

    // Arrow interop - direct cuDF calls
    std::unique_ptr<Table> table_from_arrow_host(
        uint8_t const *schema_ptr,
        uint8_t const *device_array_ptr,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr);

    std::unique_ptr<Column> column_from_arrow(
        uint8_t const *schema_ptr,
        uint8_t const *array_ptr,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr);

    std::unique_ptr<Table> concat_table_views(rust::Slice<const std::unique_ptr<TableView>> views);

    std::unique_ptr<Table> concat_table_views_on(
        rust::Slice<const std::unique_ptr<TableView>> views,
        const CudaStream &stream);

    std::unique_ptr<Column> concat_column_views(rust::Slice<const std::unique_ptr<ColumnView>> views);

    std::unique_ptr<Column> concat_column_views_on(
        rust::Slice<const std::unique_ptr<ColumnView>> views,
        const CudaStream &stream);

    std::unique_ptr<Column> make_column_from_scalar(const Scalar &scalar, size_t size);

    std::unique_ptr<Column> sequence(size_t size, const Scalar &init, const Scalar &step);

    // Gather rows from a table based on a gather map (column of indices)
    std::unique_ptr<Table> gather(const TableView &source_table, const ColumnView &gather_map);

    std::unique_ptr<Table> gather_on(
        const TableView &source_table,
        const ColumnView &gather_map,
        const CudaStream &stream);

    std::unique_ptr<Table> gather_with_policy(
        const TableView &source_table,
        const ColumnView &gather_map,
        int32_t out_of_bounds_policy);

    std::unique_ptr<Table> gather_with_policy_on(
        const TableView &source_table,
        const ColumnView &gather_map,
        int32_t out_of_bounds_policy,
        const CudaStream &stream);

    std::unique_ptr<Table> scatter_scalars(
        rust::Slice<const Scalar *const> source,
        const ColumnView &indices,
        const TableView &target);

    std::unique_ptr<Table> distinct(
        const TableView &input,
        rust::Slice<const int32_t> keys,
        int32_t keep,
        int32_t nulls_equal,
        int32_t nans_equal);

    // Create a sliced view of a column
    std::unique_ptr<ColumnView> slice_column(const ColumnView &column, size_t offset, size_t length);

    // Utility functions
    rust::String get_cudf_version();

    // Configure cuDF's default pinned-memory resource pool.
    bool config_default_pinned_memory_resource(size_t pool_size_bytes);

    // Set cuDF's host allocation threshold for pinned memory.
    void set_allocate_host_as_pinned_threshold(size_t threshold_bytes);
} // namespace libcudf_bridge
