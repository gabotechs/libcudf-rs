#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include "rust/cxx.h"

// Forward declarations of Arrow C ABI types
struct ArrowSchema;
struct ArrowArray;
struct ArrowDeviceArray;

// Forward declarations of cuDF types
namespace cudf {
    class table;
    class column;
    class table_view;
    class column_view;
    struct column_metadata;
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

// Direct cuDF operations - 1:1 mappings
std::unique_ptr<Table> apply_boolean_mask(const Table& table, const Column& boolean_mask);
std::unique_ptr<Column> table_get_column(const Table& table, size_t index);

// Arrow interop - direct cuDF calls
std::unique_ptr<Table> from_arrow_host(uint8_t* schema_ptr, uint8_t* device_array_ptr);
void to_arrow_schema(const Table& table, uint8_t* out_schema_ptr);
void to_arrow_host_array(const Table& table, uint8_t* out_array_ptr);

// Utility functions
rust::String get_cudf_version();

} // namespace libcudf_bridge
