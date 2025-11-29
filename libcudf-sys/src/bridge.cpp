#include "bridge.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/table/table.hpp>
#include <cudf/column/column.hpp>
#include <cudf/io/parquet.hpp>
#include <cudf/version_config.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/copying.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/types.hpp>
#include <cudf/interop.hpp>
#include <cudf/reduction.hpp>
#include <cudf/aggregation.hpp>
#include <cudf/scalar/scalar.hpp>

#include <rmm/device_buffer.hpp>
#include <nanoarrow/nanoarrow.h>
#include <nanoarrow/nanoarrow_device.h>
#include <stdexcept>
#include <sstream>
#include <vector>

namespace libcudf_bridge {

// Table implementation
Table::Table() : inner(nullptr) {}

Table::~Table() = default;

size_t Table::num_columns() const {
    if (!inner) {
        return 0;
    }
    return inner->num_columns();
}

size_t Table::num_rows() const {
    if (!inner) {
        return 0;
    }
    return inner->num_rows();
}

// Column implementation
Column::Column() : inner(nullptr) {}

Column::~Column() = default;

size_t Column::size() const {
    if (!inner) {
        return 0;
    }
    return inner->size();
}

// Factory functions
std::unique_ptr<Table> create_empty_table() {
    auto table = std::make_unique<Table>();
    std::vector<std::unique_ptr<cudf::column>> columns;
    table->inner = std::make_unique<cudf::table>(std::move(columns));
    return table;
}

// Parquet I/O
std::unique_ptr<Table> read_parquet(rust::Str filename) {
    std::string filename_str(filename.data(), filename.size());
    auto options = cudf::io::parquet_reader_options::builder(cudf::io::source_info{filename_str});
    auto result = cudf::io::read_parquet(options.build());

    auto table = std::make_unique<Table>();
    table->inner = std::move(result.tbl);
    return table;
}

void write_parquet(const Table& table, rust::Str filename) {
    std::string filename_str(filename.data(), filename.size());
    auto view = table.inner->view();
    auto options = cudf::io::parquet_writer_options::builder(
        cudf::io::sink_info{filename_str},
        view
    );
    cudf::io::write_parquet(options.build());
}

// Direct cuDF operations - 1:1 mappings
std::unique_ptr<Table> apply_boolean_mask(const Table& table, const Column& boolean_mask) {
    auto result = std::make_unique<Table>();
    result->inner = cudf::apply_boolean_mask(table.inner->view(), boolean_mask.inner->view());
    return result;
}

std::unique_ptr<Column> table_get_column(const Table& table, size_t index) {
    auto result = std::make_unique<Column>();
    result->inner = std::make_unique<cudf::column>(table.inner->view().column(index));
    return result;
}

// Arrow interop - direct cuDF calls
std::unique_ptr<Table> from_arrow_host(uint8_t* schema_ptr, uint8_t* device_array_ptr) {
    auto* schema = reinterpret_cast<ArrowSchema*>(schema_ptr);
    auto* device_array = reinterpret_cast<ArrowDeviceArray*>(device_array_ptr);

    auto result = std::make_unique<Table>();
    result->inner = cudf::from_arrow_host(schema, device_array);
    return result;
}

void to_arrow_schema(const Table& table, uint8_t* out_schema_ptr) {
    auto table_view = table.inner->view();
    std::vector<cudf::column_metadata> metadata(table_view.num_columns());

    auto schema_unique = cudf::to_arrow_schema(table_view, cudf::host_span<cudf::column_metadata const>(metadata));
    auto* out_schema = reinterpret_cast<ArrowSchema*>(out_schema_ptr);
    *out_schema = *schema_unique.get();
    schema_unique.release();
}

void to_arrow_host_array(const Table& table, uint8_t* out_array_ptr) {
    auto device_array_unique = cudf::to_arrow_host(table.inner->view());
    auto* out_array = reinterpret_cast<ArrowDeviceArray*>(out_array_ptr);
    *out_array = *device_array_unique.get();
    device_array_unique.release();
}

rust::String get_cudf_version() {
    std::ostringstream version;
    version << CUDF_VERSION_MAJOR << "."
            << CUDF_VERSION_MINOR << "."
            << CUDF_VERSION_PATCH;
    return rust::String(version.str());
}

} // namespace libcudf_bridge
