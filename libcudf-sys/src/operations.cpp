#include "operations.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/table/table.hpp>
#include <cudf/column/column.hpp>
#include <cudf/concatenate.hpp>
#include <cudf/copying.hpp>
#include <cudf/interop.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/version_config.hpp>

#include <nanoarrow/nanoarrow.h>

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

    // Direct cuDF operations - 1:1 mappings
    std::unique_ptr<Table> apply_boolean_mask(const TableView &table, const ColumnView &boolean_mask) {
        auto result = std::make_unique<Table>();
        result->inner = cudf::apply_boolean_mask(*table.inner, *boolean_mask.inner);
        return result;
    }

    // Gather rows from a table based on a gather map
    std::unique_ptr<Table> gather(const TableView &source_table, const ColumnView &gather_map) {
        auto result = std::make_unique<Table>();
        result->inner = cudf::gather(
            *source_table.inner,
            *gather_map.inner,
            cudf::out_of_bounds_policy::DONT_CHECK
        );
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
