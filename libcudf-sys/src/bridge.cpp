#include "bridge.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/table/table.hpp>
#include <cudf/column/column.hpp>
#include <cudf/io/parquet.hpp>
#include <cudf/version_config.hpp>

#include <stdexcept>
#include <sstream>

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

rust::String get_cudf_version() {
    std::ostringstream version;
    version << CUDF_VERSION_MAJOR << "."
            << CUDF_VERSION_MINOR << "."
            << CUDF_VERSION_PATCH;
    return rust::String(version.str());
}

} // namespace libcudf_bridge
