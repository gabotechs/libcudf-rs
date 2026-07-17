#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "column.h"
#include "memory_resource.h"
#include "stream.h"
#include "table.h"

#include <cudf/interop.hpp>

namespace libcudf_bridge {
    [[nodiscard]] std::unique_ptr<Table> from_arrow_host(
        std::uint8_t const* schema_ptr,
        std::uint8_t const* device_array_ptr,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr);

    [[nodiscard]] std::unique_ptr<Column> from_arrow_column(
        std::uint8_t const* schema_ptr,
        std::uint8_t const* array_ptr,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr);

    struct ColumnMetadata {
        cudf::column_metadata inner;

        explicit ColumnMetadata(cudf::column_metadata metadata);

        [[nodiscard]] rust::String name() const;
        void set_name(rust::Str value);
        [[nodiscard]] rust::String timezone() const;
        void set_timezone(rust::Str value);
        [[nodiscard]] bool has_precision() const;
        [[nodiscard]] std::int32_t precision() const;
        void set_precision(std::int32_t value);
        void clear_precision();
        [[nodiscard]] std::size_t children_len() const;
        [[nodiscard]] std::unique_ptr<ColumnMetadata> child(std::size_t index) const;
        void set_child(std::size_t index, const ColumnMetadata& child);
        void push_child(const ColumnMetadata& child);
    };

    struct ColumnMetadataVector {
        std::vector<cudf::column_metadata> inner;

        explicit ColumnMetadataVector(std::vector<cudf::column_metadata> metadata);

        [[nodiscard]] std::size_t len() const;
        [[nodiscard]] bool is_empty() const;
        [[nodiscard]] std::unique_ptr<ColumnMetadata> get(std::size_t index) const;
        void set(std::size_t index, const ColumnMetadata& metadata);
        void push(const ColumnMetadata& metadata);
    };

    // cxx cannot represent recursive column_metadata values or optional fields
    // directly, so these factories and accessors mechanically bridge them.
    [[nodiscard]] std::unique_ptr<ColumnMetadata> make_column_metadata();
    [[nodiscard]] std::unique_ptr<ColumnMetadata> make_column_metadata_with_name(rust::Str name);
    [[nodiscard]] std::unique_ptr<ColumnMetadataVector> make_column_metadata_vector();

    [[nodiscard]] std::unique_ptr<ColumnMetadata> get_column_metadata(const ColumnView& input);

    [[nodiscard]] std::unique_ptr<ColumnMetadataVector> get_table_metadata(const TableView& input);

    void to_arrow_schema(
        const TableView& input,
        const ColumnMetadataVector& metadata,
        std::uint8_t* out_schema_ptr);

    void to_arrow_host(
        const TableView& input,
        std::uint8_t* out_array_ptr,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr);

    void to_arrow_host(
        const ColumnView& input,
        std::uint8_t* out_array_ptr,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr);
}  // namespace libcudf_bridge
