#include "operations.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/table/table.hpp>
#include <cudf/column/column.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/concatenate.hpp>
#include <cudf/copying.hpp>
#include <cudf/filling.hpp>
#include <cudf/interop.hpp>
#include <cudf/scalar/scalar.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/utilities/default_stream.hpp>
#include <cudf/utilities/pinned_memory.hpp>
#include <cudf/version_config.hpp>

#include <nanoarrow/nanoarrow.h>

#include <functional>
#include <limits>
#include <sstream>

namespace libcudf_bridge {
    // Factory functions
    std::unique_ptr<Table> create_empty_table() {
        auto table = std::make_unique<Table>();
        std::vector<std::unique_ptr<cudf::column> > columns;
        table->inner = std::make_unique<cudf::table>(std::move(columns));
        return table;
    }

    std::unique_ptr<Table> create_table_from_columns_move(rust::Slice<Column *const> columns) {
        std::vector<std::unique_ptr<cudf::column> > cudf_columns;
        cudf_columns.reserve(columns.size());

        // Take ownership of columns by moving from each pointer
        for (auto *col: columns) {
            cudf_columns.push_back(std::move(col->inner));
        }

        auto table = std::make_unique<Table>();
        table->inner = std::make_unique<cudf::table>(std::move(cudf_columns));
        return table;
    }

    std::unique_ptr<Table> concat_table_views(rust::Slice<const std::unique_ptr<TableView>> views) {
        std::vector<cudf::table_view> table_views;
        table_views.reserve(views.size());

        // Take ownership of tables by moving out of each unique pointer
        for (auto &col: views) {
            table_views.push_back(std::move(*col->inner));
        }

        auto table = std::make_unique<Table>();
        table->inner = cudf::concatenate(table_views);
        return table;
    }

    std::unique_ptr<Column> concat_column_views(rust::Slice<const std::unique_ptr<ColumnView>> views) {
        std::vector<cudf::column_view> table_views;
        table_views.reserve(views.size());

        // Take ownership of columns by moving out of each unique pointer
        for (auto &col: views) {
            table_views.push_back(std::move(*col->inner));
        }

        auto table = std::make_unique<Column>();
        table->inner = cudf::concatenate(table_views);
        return table;
    }

    std::unique_ptr<Column> make_column_from_scalar(const Scalar &scalar, size_t size) {
        if (size > static_cast<size_t>(std::numeric_limits<cudf::size_type>::max())) {
            throw std::out_of_range("column size exceeds cudf::size_type");
        }

        auto result = std::make_unique<Column>();
        result->inner = cudf::make_column_from_scalar(
            *scalar.inner,
            static_cast<cudf::size_type>(size));
        return result;
    }

    std::unique_ptr<Column> sequence(size_t size, const Scalar &init, const Scalar &step) {
        if (size > static_cast<size_t>(std::numeric_limits<cudf::size_type>::max())) {
            throw std::out_of_range("sequence size exceeds cudf::size_type");
        }

        auto stream = cudf::get_default_stream();
        auto result = std::make_unique<Column>();
        result->inner = cudf::sequence(
            static_cast<cudf::size_type>(size),
            *init.inner,
            *step.inner,
            stream);
        return result;
    }

    // Direct cuDF operations - 1:1 mappings
    std::unique_ptr<Table> apply_boolean_mask(const TableView &table, const ColumnView &boolean_mask) {
        auto result = std::make_unique<Table>();
        result->inner = cudf::apply_boolean_mask(*table.inner, *boolean_mask.inner);
        return result;
    }

    // Gather rows from a table based on a gather map
    std::unique_ptr<Table> gather(const TableView &source_table, const ColumnView &gather_map) {
        return gather_with_policy(source_table, gather_map,
                                  static_cast<int32_t>(cudf::out_of_bounds_policy::DONT_CHECK));
    }

    std::unique_ptr<Table> gather_with_policy(
        const TableView &source_table,
        const ColumnView &gather_map,
        int32_t out_of_bounds_policy) {
        auto result = std::make_unique<Table>();
        result->inner = cudf::gather(
            *source_table.inner,
            *gather_map.inner,
            static_cast<cudf::out_of_bounds_policy>(out_of_bounds_policy)
        );
        return result;
    }

    std::unique_ptr<Table> scatter_scalars(
        rust::Slice<const Scalar *const> source,
        const ColumnView &indices,
        const TableView &target) {
        std::vector<std::reference_wrapper<cudf::scalar const>> scalars;
        scalars.reserve(source.size());
        for (auto *scalar: source) {
            scalars.emplace_back(*scalar->inner);
        }

        auto result = std::make_unique<Table>();
        result->inner = cudf::scatter(scalars, *indices.inner, *target.inner);
        return result;
    }

    std::unique_ptr<Table> distinct(
        const TableView &input,
        rust::Slice<const int32_t> keys,
        int32_t keep,
        int32_t nulls_equal,
        int32_t nans_equal) {
        std::vector<cudf::size_type> key_indices;
        key_indices.reserve(keys.size());
        for (auto key: keys) {
            key_indices.push_back(static_cast<cudf::size_type>(key));
        }

        auto result = std::make_unique<Table>();
        result->inner = cudf::distinct(
            *input.inner,
            key_indices,
            static_cast<cudf::duplicate_keep_option>(keep),
            static_cast<cudf::null_equality>(nulls_equal),
            static_cast<cudf::nan_equality>(nans_equal));
        return result;
    }

    // Create a sliced view of a column
    std::unique_ptr<ColumnView> slice_column(const ColumnView &column, size_t offset, size_t length) {
        if (!column.inner) {
            throw std::runtime_error("Cannot slice null column view");
        }
        if (offset + length > static_cast<size_t>(column.inner->size())) {
            throw std::out_of_range("Slice bounds out of range");
        }

        // Use cuDF's native slice function from cudf/copying.hpp
        // slice() takes pairs of [start, end) indices and returns a vector of views
        auto start = static_cast<cudf::size_type>(offset);
        auto end = static_cast<cudf::size_type>(offset + length);
        std::vector indices = {start, end};

        auto sliced_views = cudf::slice(*column.inner, indices);

        // We expect exactly one view back since we provided one [start, end) pair
        if (sliced_views.empty()) {
            throw std::runtime_error("cudf::slice returned no views");
        }

        auto result = std::make_unique<ColumnView>();
        result->inner = std::make_unique<cudf::column_view>(sliced_views.at(0));
        return result;
    }

    rust::String get_cudf_version() {
        std::ostringstream version;
        version << CUDF_VERSION_MAJOR << "."
                << CUDF_VERSION_MINOR << "."
                << CUDF_VERSION_PATCH;
        return {version.str()};
    }

    bool config_default_pinned_memory_resource(size_t pool_size_bytes) {
        return cudf::config_default_pinned_memory_resource({.pool_size = pool_size_bytes});
    }

    void set_allocate_host_as_pinned_threshold(size_t threshold_bytes) {
        cudf::set_allocate_host_as_pinned_threshold(threshold_bytes);
    }

    // Arrow interop - convert Arrow data to cuDF table
    std::unique_ptr<Table> table_from_arrow_host(uint8_t const *schema_ptr, uint8_t const *device_array_ptr) {
        auto *schema = reinterpret_cast<const ArrowSchema *>(schema_ptr);
        auto *device_array = reinterpret_cast<const ArrowDeviceArray *>(device_array_ptr);

        auto result = std::make_unique<Table>();
        result->inner = cudf::from_arrow_host(schema, device_array);
        return result;
    }

    // Arrow interop - convert Arrow array to cuDF column
    std::unique_ptr<Column> column_from_arrow(uint8_t const *schema_ptr, uint8_t const *array_ptr) {
        auto *schema = reinterpret_cast<const ArrowSchema *>(schema_ptr);
        auto *array = reinterpret_cast<const ArrowArray *>(array_ptr);

        auto result = std::make_unique<Column>();
        result->inner = cudf::from_arrow_column(schema, array);
        return result;
    }
} // namespace libcudf_bridge
