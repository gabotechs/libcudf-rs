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

#include <rmm/device_buffer.hpp>
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

// Table operations
std::unique_ptr<Table> select_columns(const Table& table, rust::Slice<const size_t> indices) {
    if (!table.inner) {
        throw std::runtime_error("Table is null");
    }

    auto view = table.inner->view();
    std::vector<cudf::size_type> column_indices;
    column_indices.reserve(indices.size());

    for (size_t idx : indices) {
        if (idx >= view.num_columns()) {
            throw std::out_of_range("Column index out of range");
        }
        column_indices.push_back(static_cast<cudf::size_type>(idx));
    }

    // Select columns by creating a new table with only the specified columns
    std::vector<std::unique_ptr<cudf::column>> selected_columns;
    selected_columns.reserve(column_indices.size());

    for (auto idx : column_indices) {
        selected_columns.push_back(std::make_unique<cudf::column>(view.column(idx)));
    }

    auto result = std::make_unique<Table>();
    result->inner = std::make_unique<cudf::table>(std::move(selected_columns));
    return result;
}

std::unique_ptr<Column> get_column(const Table& table, size_t index) {
    if (!table.inner) {
        throw std::runtime_error("Table is null");
    }

    auto view = table.inner->view();
    if (index >= view.num_columns()) {
        throw std::out_of_range("Column index out of range");
    }

    auto result = std::make_unique<Column>();
    result->inner = std::make_unique<cudf::column>(view.column(index));
    return result;
}

std::unique_ptr<Table> filter(const Table& table, const Column& boolean_mask) {
    if (!table.inner) {
        throw std::runtime_error("Table is null");
    }
    if (!boolean_mask.inner) {
        throw std::runtime_error("Boolean mask is null");
    }

    auto table_view = table.inner->view();
    auto mask_view = boolean_mask.inner->view();

    // Verify the mask has the same number of rows as the table
    if (mask_view.size() != table_view.num_rows()) {
        throw std::runtime_error("Boolean mask must have same number of rows as table");
    }

    // Apply the boolean mask using cuDF's stream_compaction
    auto filtered_table = cudf::apply_boolean_mask(table_view, mask_view);

    auto result = std::make_unique<Table>();
    result->inner = std::move(filtered_table);
    return result;
}

// Column creation
std::unique_ptr<Column> create_boolean_column(rust::Slice<const bool> data) {
    cudf::size_type size = static_cast<cudf::size_type>(data.size());

    // Convert bool to int8_t (BOOL8 in cuDF)
    std::vector<int8_t> host_data(size);
    for (size_t i = 0; i < data.size(); ++i) {
        host_data[i] = data[i] ? 1 : 0;
    }

    // Create a mutable column
    auto column = cudf::make_numeric_column(
        cudf::data_type{cudf::type_id::BOOL8},
        size,
        cudf::mask_state::UNALLOCATED
    );

    // Copy data from host to device
    cudaMemcpy(
        column->mutable_view().data<int8_t>(),
        host_data.data(),
        size * sizeof(int8_t),
        cudaMemcpyHostToDevice
    );

    auto result = std::make_unique<Column>();
    result->inner = std::move(column);
    return result;
}

rust::String get_cudf_version() {
    std::ostringstream version;
    version << CUDF_VERSION_MAJOR << "."
            << CUDF_VERSION_MINOR << "."
            << CUDF_VERSION_PATCH;
    return rust::String(version.str());
}

} // namespace libcudf_bridge
