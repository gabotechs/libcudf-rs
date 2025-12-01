#include "bridge.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/table/table.hpp>
#include <cudf/column/column.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/interop.hpp>
#include <cudf/version_config.hpp>

#include <nanoarrow/nanoarrow.h>
#include <nanoarrow/nanoarrow_device.h>
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

    // Direct cuDF operations - 1:1 mappings
    std::unique_ptr<Table> apply_boolean_mask(const Table &table, const Column &boolean_mask) {
        auto result = std::make_unique<Table>();
        result->inner = cudf::apply_boolean_mask(table.inner->view(), boolean_mask.inner->view());
        return result;
    }

    // Arrow interop - direct cuDF calls
    std::unique_ptr<Table> from_arrow_host(uint8_t const *schema_ptr, uint8_t const *device_array_ptr) {
        const auto *schema = reinterpret_cast<ArrowSchema const *>(schema_ptr);
        const auto *device_array = reinterpret_cast<ArrowDeviceArray const *>(device_array_ptr);

        auto result = std::make_unique<Table>();
        result->inner = cudf::from_arrow_host(schema, device_array);
        return result;
    }

    void to_arrow_schema(const Table &table, uint8_t *out_schema_ptr) {
        const auto table_view = table.inner->view();
        std::vector<cudf::column_metadata> metadata(table_view.num_columns());

        auto schema_unique = cudf::to_arrow_schema(table_view, cudf::host_span<cudf::column_metadata const>(metadata));
        auto *out_schema = reinterpret_cast<ArrowSchema *>(out_schema_ptr);
        *out_schema = *schema_unique.get();
        schema_unique.release();
    }

    void to_arrow_host_array(const Table &table, uint8_t *out_array_ptr) {
        auto device_array_unique = cudf::to_arrow_host(table.inner->view());
        auto *out_array = reinterpret_cast<ArrowDeviceArray *>(out_array_ptr);
        *out_array = *device_array_unique.get();
        device_array_unique.release();
    }

    rust::String get_cudf_version() {
        std::ostringstream version;
        version << CUDF_VERSION_MAJOR << "."
                << CUDF_VERSION_MINOR << "."
                << CUDF_VERSION_PATCH;
        return {version.str()};
    }
} // namespace libcudf_bridge
