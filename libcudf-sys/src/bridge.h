#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include "rust/cxx.h"

// Forward declarations of cuDF types
namespace cudf {
    class table;
    class column;
    class table_view;
    class column_view;
    namespace io {
        struct table_with_metadata;
    }
}

namespace libcudf_bridge {

// Opaque wrapper for cuDF table
// This allows us to safely pass cuDF objects across the FFI boundary
struct Table {
    std::unique_ptr<cudf::table> inner;

    Table();
    ~Table();

    // Get number of columns
    size_t num_columns() const;

    // Get number of rows
    size_t num_rows() const;
};

// Opaque wrapper for cuDF column
struct Column {
    std::unique_ptr<cudf::column> inner;

    Column();
    ~Column();

    // Get number of elements
    size_t size() const;
};

// Factory functions for creating cuDF objects
std::unique_ptr<Table> create_empty_table();

// Example: Read CSV file
std::unique_ptr<Table> read_csv(rust::Str filename);

// Example: Write CSV file
void write_csv(const Table& table, rust::Str filename);

// Get cuDF version information
rust::String get_cudf_version();

} // namespace libcudf_bridge
