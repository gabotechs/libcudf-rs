#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "ast.h"
#include "data_type.h"
#include "memory_resource.h"
#include "rust/cxx.h"
#include "stream.h"
#include "table.h"

#include <cudf/io/parquet.hpp>

namespace libcudf_bridge {
    struct TableWithMetadata;

    // Opaque wrapper for cudf::io::source_info.
    struct SourceInfo {
        cudf::io::source_info inner;

        SourceInfo();

        explicit SourceInfo(cudf::io::source_info source);

        ~SourceInfo();

        [[nodiscard]] size_t num_sources() const;
    };

    // Opaque wrapper for cudf::io::sink_info.
    struct SinkInfo {
        cudf::io::sink_info inner;

        SinkInfo();

        explicit SinkInfo(cudf::io::sink_info sink);

        ~SinkInfo();

        [[nodiscard]] size_t num_sinks() const;
    };

    // Opaque wrapper for cudf::io::parquet_reader_options.
    struct ParquetReaderOptions {
        cudf::io::parquet_reader_options inner;

        ParquetReaderOptions();

        explicit ParquetReaderOptions(cudf::io::parquet_reader_options options);

        ~ParquetReaderOptions();

        void set_source(const SourceInfo& source);

        void set_columns(rust::Vec<rust::String> col_names);

        void set_row_groups(rust::Vec<int32_t> row_group_indices, rust::Vec<size_t> source_offsets);

        void set_filter(const AstExpressionTree& filter);

        void enable_convert_strings_to_categories(bool val);

        void enable_use_pandas_metadata(bool val);

        void enable_use_arrow_schema(bool val);

        void enable_allow_mismatched_pq_schemas(bool val);

        void enable_ignore_missing_columns(bool val);

        void set_skip_rows(int64_t val);

        void set_num_rows(int64_t val);

        void set_skip_bytes(size_t val);

        void set_num_bytes(size_t val);

        void set_timestamp_type(const DataType& type);
    };

    // Opaque wrapper for cudf::io::chunked_parquet_reader.
    struct ChunkedParquetReader {
        std::unique_ptr<cudf::io::chunked_parquet_reader> inner;

        ChunkedParquetReader(
            size_t chunk_read_limit,
            size_t pass_read_limit,
            const ParquetReaderOptions& options,
            const CudaStreamView& stream,
            const DeviceAsyncResourceRef& mr);

        ~ChunkedParquetReader();

        [[nodiscard]] bool has_next() const;

        std::unique_ptr<TableWithMetadata> read_chunk() const;
    };

    // Opaque wrapper for cudf::io::parquet_writer_options.
    struct ParquetWriterOptions {
        cudf::io::parquet_writer_options inner;

        ParquetWriterOptions();

        explicit ParquetWriterOptions(cudf::io::parquet_writer_options options);

        ~ParquetWriterOptions();
    };

    // Opaque wrapper for cudf::io::table_with_metadata.
    struct TableWithMetadata {
        cudf::io::table_with_metadata inner;

        explicit TableWithMetadata(cudf::io::table_with_metadata result);

        ~TableWithMetadata();

        std::unique_ptr<Table> release_table();

        [[nodiscard]] size_t num_rows_per_source_count() const;

        [[nodiscard]] size_t num_rows_per_source(size_t index) const;

        [[nodiscard]] int32_t num_input_row_groups() const;

        [[nodiscard]] size_t schema_info_count() const;

        [[nodiscard]] rust::String schema_info_name(size_t index) const;
    };

    // Opaque owning wrapper for the file metadata byte vector returned by write_parquet.
    struct HostByteVector {
        std::unique_ptr<std::vector<uint8_t>> inner;

        explicit HostByteVector(std::unique_ptr<std::vector<uint8_t>> bytes);

        ~HostByteVector();

        [[nodiscard]] size_t size() const;
    };

    std::unique_ptr<SourceInfo> source_info_from_file_path(rust::Str file_path);

    std::unique_ptr<SourceInfo> source_info_from_file_paths(rust::Vec<rust::String> file_paths);

    std::unique_ptr<SinkInfo> sink_info_from_file_path(rust::Str file_path);

    std::unique_ptr<SinkInfo> sink_info_from_file_paths(rust::Vec<rust::String> file_paths);

    std::unique_ptr<ParquetReaderOptions> parquet_reader_options_create(const SourceInfo& source);

    std::unique_ptr<ParquetWriterOptions> parquet_writer_options_create(
        const SinkInfo& sink,
        const TableView& table);

    std::unique_ptr<TableWithMetadata> read_parquet(
        const ParquetReaderOptions& options,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr);

    std::unique_ptr<ChunkedParquetReader> chunked_parquet_reader_create(
        size_t chunk_read_limit,
        size_t pass_read_limit,
        const ParquetReaderOptions& options,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr);

    std::unique_ptr<HostByteVector> write_parquet(
        const ParquetWriterOptions& options,
        const CudaStreamView& stream);
} // namespace libcudf_bridge
