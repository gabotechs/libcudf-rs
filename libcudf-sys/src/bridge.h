#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include "rust/cxx.h"

// Forward declarations of Arrow C ABI types
struct ArrowSchema;
struct ArrowArray;

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

// Table factory functions
std::unique_ptr<Table> create_empty_table();

// Parquet I/O
std::unique_ptr<Table> read_parquet(rust::Str filename);
void write_parquet(const Table& table, rust::Str filename);

// Table operations
std::unique_ptr<Table> select_columns(const Table& table, rust::Slice<const size_t> indices);
std::unique_ptr<Column> get_column(const Table& table, size_t index);
std::unique_ptr<Table> filter(const Table& table, const Column& boolean_mask);

// Column creation
std::unique_ptr<Column> create_boolean_column(rust::Slice<const bool> data);

// Arrow interop
std::unique_ptr<Table> from_arrow(uint8_t* schema_ptr, uint8_t* array_ptr);
void to_arrow(const Table& table, uint8_t* schema_ptr, uint8_t* array_ptr);

// Utility functions
rust::String get_cudf_version();

} // namespace libcudf_bridge
