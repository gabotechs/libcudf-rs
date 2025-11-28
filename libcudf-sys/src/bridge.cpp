#include "bridge.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/table/table.hpp>
#include <cudf/column/column.hpp>
#include <cudf/io/csv.hpp>
#include <cudf/utilities/error.hpp>
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

std::unique_ptr<Table> read_csv(rust::Str filename) {
    try {
        std::string filename_str(filename.data(), filename.size());

        // Configure CSV reader options
        auto options = cudf::io::csv_reader_options::builder(cudf::io::source_info{filename_str});

        // Read the CSV file
        auto result = cudf::io::read_csv(options.build());

        // Wrap in our Table type
        auto table = std::make_unique<Table>();
        table->inner = std::move(result.tbl);

        return table;
    } catch (const cudf::logic_error& e) {
        std::ostringstream oss;
        oss << "cuDF logic error: " << e.what();
        throw std::runtime_error(oss.str());
    } catch (const cudf::cuda_error& e) {
        std::ostringstream oss;
        oss << "CUDA error: " << e.what();
        throw std::runtime_error(oss.str());
    } catch (const std::exception& e) {
        std::ostringstream oss;
        oss << "Error reading CSV: " << e.what();
        throw std::runtime_error(oss.str());
    }
}

void write_csv(const Table& table, rust::Str filename) {
    try {
        if (!table.inner) {
            throw std::runtime_error("Cannot write null table");
        }

        std::string filename_str(filename.data(), filename.size());

        // Create a table view for writing
        auto view = table.inner->view();

        // Configure CSV writer options
        auto options = cudf::io::csv_writer_options::builder(
            cudf::io::sink_info{filename_str},
            view
        );

        // Write the CSV file
        cudf::io::write_csv(options.build());

    } catch (const cudf::logic_error& e) {
        std::ostringstream oss;
        oss << "cuDF logic error: " << e.what();
        throw std::runtime_error(oss.str());
    } catch (const cudf::cuda_error& e) {
        std::ostringstream oss;
        oss << "CUDA error: " << e.what();
        throw std::runtime_error(oss.str());
    } catch (const std::exception& e) {
        std::ostringstream oss;
        oss << "Error writing CSV: " << e.what();
        throw std::runtime_error(oss.str());
    }
}

rust::String get_cudf_version() {
    std::ostringstream version;
    version << CUDF_VERSION_MAJOR << "."
            << CUDF_VERSION_MINOR << "."
            << CUDF_VERSION_PATCH;
    return rust::String(version.str());
}

} // namespace libcudf_bridge
