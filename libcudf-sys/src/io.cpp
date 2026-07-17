#include "io.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/io/parquet.hpp>
#include <stdexcept>

namespace libcudf_bridge {
    namespace {
        std::string to_std_string(const rust::Str value) {
            return std::string(value.data(), value.size());
        }

        std::vector<std::string> to_std_strings(const rust::Vec<rust::String>& values) {
            std::vector<std::string> result;
            result.reserve(values.size());
            for (const auto& value: values) {
                result.emplace_back(static_cast<std::string>(value));
            }
            return result;
        }

        std::vector<std::vector<cudf::size_type>> to_row_groups(
            const rust::Vec<int32_t>& row_group_indices,
            const rust::Vec<size_t>& source_offsets) {
            if (source_offsets.empty() || source_offsets.front() != 0) {
                throw std::invalid_argument("row group source offsets must start at zero");
            }

            std::vector<std::vector<cudf::size_type>> result;
            result.reserve(source_offsets.size() - 1);
            size_t previous = 0;
            for (size_t offset_index = 1; offset_index < source_offsets.size(); ++offset_index) {
                auto next = source_offsets[offset_index];
                if (next < previous || next > row_group_indices.size()) {
                    throw std::invalid_argument("invalid row group source offset");
                }

                std::vector<cudf::size_type> groups;
                groups.reserve(next - previous);
                for (size_t index = previous; index < next; ++index) {
                    groups.push_back(static_cast<cudf::size_type>(row_group_indices[index]));
                }
                result.push_back(std::move(groups));
                previous = next;
            }

            if (previous != row_group_indices.size()) {
                throw std::invalid_argument("row group source offsets do not cover all indices");
            }
            return result;
        }
    } // namespace

    ColumnNameInfo::ColumnNameInfo(cudf::io::column_name_info info) : inner(std::move(info)) {}
    ColumnNameInfo::~ColumnNameInfo() = default;
    rust::String ColumnNameInfo::name() const { return inner.name; }
    bool ColumnNameInfo::has_is_nullable() const { return inner.is_nullable.has_value(); }
    bool ColumnNameInfo::is_nullable() const { return inner.is_nullable.value(); }
    bool ColumnNameInfo::has_is_binary() const { return inner.is_binary.has_value(); }
    bool ColumnNameInfo::is_binary() const { return inner.is_binary.value(); }
    bool ColumnNameInfo::has_type_length() const { return inner.type_length.has_value(); }
    int32_t ColumnNameInfo::type_length() const { return inner.type_length.value(); }
    size_t ColumnNameInfo::children_len() const { return inner.children.size(); }

    std::unique_ptr<ColumnNameInfo> ColumnNameInfo::child(size_t index) const {
        return std::make_unique<ColumnNameInfo>(inner.children.at(index));
    }

    KeyValuePairs::KeyValuePairs(const std::map<std::string, std::string>& values)
        : inner(values.begin(), values.end()) {}
    KeyValuePairs::KeyValuePairs(const std::unordered_map<std::string, std::string>& values)
        : inner(values.begin(), values.end()) {}
    KeyValuePairs::~KeyValuePairs() = default;
    size_t KeyValuePairs::len() const { return inner.size(); }
    bool KeyValuePairs::is_empty() const { return inner.empty(); }
    rust::String KeyValuePairs::key(size_t index) const { return inner.at(index).first; }
    rust::String KeyValuePairs::value(size_t index) const { return inner.at(index).second; }

    TableMetadata::TableMetadata(cudf::io::table_metadata metadata)
        : inner(std::move(metadata)) {}
    TableMetadata::~TableMetadata() = default;
    size_t TableMetadata::schema_info_len() const { return inner.schema_info.size(); }

    std::unique_ptr<ColumnNameInfo> TableMetadata::schema_info(size_t index) const {
        return std::make_unique<ColumnNameInfo>(inner.schema_info.at(index));
    }

    size_t TableMetadata::num_rows_per_source_len() const {
        return inner.num_rows_per_source.size();
    }

    size_t TableMetadata::num_rows_per_source(size_t index) const {
        return inner.num_rows_per_source.at(index);
    }

    std::unique_ptr<KeyValuePairs> TableMetadata::user_data() const {
        return std::make_unique<KeyValuePairs>(inner.user_data);
    }

    size_t TableMetadata::per_file_user_data_len() const {
        return inner.per_file_user_data.size();
    }

    std::unique_ptr<KeyValuePairs> TableMetadata::per_file_user_data(size_t index) const {
        return std::make_unique<KeyValuePairs>(inner.per_file_user_data.at(index));
    }

    int32_t TableMetadata::num_input_row_groups() const {
        return inner.num_input_row_groups;
    }

    bool TableMetadata::has_num_row_groups_after_stats_filter() const {
        return inner.num_row_groups_after_stats_filter.has_value();
    }

    int32_t TableMetadata::num_row_groups_after_stats_filter() const {
        return inner.num_row_groups_after_stats_filter.value();
    }

    bool TableMetadata::has_num_row_groups_after_bloom_filter() const {
        return inner.num_row_groups_after_bloom_filter.has_value();
    }

    int32_t TableMetadata::num_row_groups_after_bloom_filter() const {
        return inner.num_row_groups_after_bloom_filter.value();
    }

    SourceInfo::SourceInfo() : inner() {}

    SourceInfo::SourceInfo(cudf::io::source_info source) : inner(std::move(source)) {}

    SourceInfo::~SourceInfo() = default;

    size_t SourceInfo::num_sources() const {
        return inner.num_sources();
    }

    SinkInfo::SinkInfo() : inner() {}

    SinkInfo::SinkInfo(cudf::io::sink_info sink) : inner(std::move(sink)) {}

    SinkInfo::~SinkInfo() = default;

    size_t SinkInfo::num_sinks() const {
        return inner.num_sinks();
    }

    ParquetReaderOptions::ParquetReaderOptions() : inner() {}

    ParquetReaderOptions::ParquetReaderOptions(cudf::io::parquet_reader_options options)
        : inner(std::move(options)) {}

    ParquetReaderOptions::~ParquetReaderOptions() = default;

    void ParquetReaderOptions::set_source(const SourceInfo& source) {
        inner.set_source(source.inner);
    }

    void ParquetReaderOptions::set_columns(rust::Vec<rust::String> col_names) {
        inner.set_columns(to_std_strings(col_names));
    }

    void ParquetReaderOptions::set_row_groups(
        rust::Vec<int32_t> row_group_indices,
        rust::Vec<size_t> source_offsets) {
        inner.set_row_groups(to_row_groups(row_group_indices, source_offsets));
    }

    void ParquetReaderOptions::set_filter(const AstExpressionTree& filter) {
        if (filter.inner.size() == 0) {
            throw std::runtime_error("Cannot set an empty AST filter on ParquetReaderOptions");
        }
        inner.set_filter(filter.inner.back());
    }

    void ParquetReaderOptions::enable_convert_strings_to_categories(bool val) {
        inner.enable_convert_strings_to_categories(val);
    }

    void ParquetReaderOptions::enable_use_pandas_metadata(bool val) {
        inner.enable_use_pandas_metadata(val);
    }

    void ParquetReaderOptions::enable_use_arrow_schema(bool val) {
        inner.enable_use_arrow_schema(val);
    }

    void ParquetReaderOptions::enable_allow_mismatched_pq_schemas(bool val) {
        inner.enable_allow_mismatched_pq_schemas(val);
    }

    void ParquetReaderOptions::enable_ignore_missing_columns(bool val) {
        inner.enable_ignore_missing_columns(val);
    }

    void ParquetReaderOptions::set_skip_rows(int64_t val) {
        inner.set_skip_rows(val);
    }

    void ParquetReaderOptions::set_num_rows(int64_t val) {
        inner.set_num_rows(val);
    }

    void ParquetReaderOptions::set_skip_bytes(size_t val) {
        inner.set_skip_bytes(val);
    }

    void ParquetReaderOptions::set_num_bytes(size_t val) {
        inner.set_num_bytes(val);
    }

    void ParquetReaderOptions::set_timestamp_type(const DataType& type) {
        inner.set_timestamp_type(type.inner);
    }

    ChunkedParquetReader::ChunkedParquetReader(
        size_t chunk_read_limit,
        size_t pass_read_limit,
        const ParquetReaderOptions& options,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr)
        : inner(std::make_unique<cudf::io::chunked_parquet_reader>(
              chunk_read_limit,
              pass_read_limit,
              options.inner,
              stream.inner,
              mr.inner)) {}

    ChunkedParquetReader::~ChunkedParquetReader() = default;

    bool ChunkedParquetReader::has_next() const {
        if (!inner) {
            throw std::runtime_error("Cannot inspect a null chunked Parquet reader");
        }
        return inner->has_next();
    }

    std::unique_ptr<TableWithMetadata> ChunkedParquetReader::read_chunk() const {
        if (!inner) {
            throw std::runtime_error("Cannot read from a null chunked Parquet reader");
        }
        auto result = inner->read_chunk();
        return std::make_unique<TableWithMetadata>(std::move(result));
    }

    ParquetWriterOptions::ParquetWriterOptions() : inner() {}

    ParquetWriterOptions::ParquetWriterOptions(cudf::io::parquet_writer_options options)
        : inner(std::move(options)) {}

    ParquetWriterOptions::~ParquetWriterOptions() = default;

    TableWithMetadata::TableWithMetadata(cudf::io::table_with_metadata result)
        : inner(std::move(result)) {}

    TableWithMetadata::~TableWithMetadata() = default;

    std::unique_ptr<Table> TableWithMetadata::release_table() {
        if (!inner.tbl) {
            throw std::runtime_error("Parquet result table has already been released");
        }
        auto table = std::make_unique<Table>();
        table->inner = std::move(inner.tbl);
        return table;
    }

    std::unique_ptr<TableMetadata> TableWithMetadata::release_metadata() {
        if (metadata_released) {
            throw std::runtime_error("Parquet result metadata has already been released");
        }
        auto metadata = std::make_unique<TableMetadata>(std::move(inner.metadata));
        metadata_released = true;
        return metadata;
    }

    HostByteVector::HostByteVector(std::unique_ptr<std::vector<uint8_t>> bytes)
        : inner(std::move(bytes)) {}

    HostByteVector::~HostByteVector() = default;

    size_t HostByteVector::size() const {
        if (!inner) {
            throw std::runtime_error("Cannot inspect a null host byte vector");
        }
        return inner->size();
    }

    std::unique_ptr<SourceInfo> source_info_from_file_path(const rust::Str file_path) {
        return std::make_unique<SourceInfo>(cudf::io::source_info{to_std_string(file_path)});
    }

    std::unique_ptr<SourceInfo> source_info_from_file_paths(rust::Vec<rust::String> file_paths) {
        return std::make_unique<SourceInfo>(cudf::io::source_info{to_std_strings(file_paths)});
    }

    std::unique_ptr<SinkInfo> sink_info_from_file_path(const rust::Str file_path) {
        return std::make_unique<SinkInfo>(cudf::io::sink_info{to_std_string(file_path)});
    }

    std::unique_ptr<SinkInfo> sink_info_from_file_paths(rust::Vec<rust::String> file_paths) {
        return std::make_unique<SinkInfo>(cudf::io::sink_info{to_std_strings(file_paths)});
    }

    std::unique_ptr<ParquetReaderOptions> parquet_reader_options_create(const SourceInfo& source) {
        return std::make_unique<ParquetReaderOptions>(
            cudf::io::parquet_reader_options::builder(source.inner).build());
    }

    std::unique_ptr<ParquetWriterOptions> parquet_writer_options_create(
        const SinkInfo& sink,
        const TableView& table) {
        if (!table.inner) {
            throw std::invalid_argument("Cannot build Parquet writer options from a null table view");
        }
        return std::make_unique<ParquetWriterOptions>(
            cudf::io::parquet_writer_options::builder(sink.inner, *table.inner).build());
    }

    std::unique_ptr<TableWithMetadata> read_parquet(
        const ParquetReaderOptions& options,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr) {
        auto result = cudf::io::read_parquet(options.inner, stream.inner, mr.inner);
        return std::make_unique<TableWithMetadata>(std::move(result));
    }

    std::unique_ptr<ChunkedParquetReader> chunked_parquet_reader_create(
        size_t chunk_read_limit,
        size_t pass_read_limit,
        const ParquetReaderOptions& options,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr) {
        return std::make_unique<ChunkedParquetReader>(
            chunk_read_limit,
            pass_read_limit,
            options,
            stream,
            mr);
    }

    std::unique_ptr<HostByteVector> write_parquet(
        const ParquetWriterOptions& options,
        const CudaStreamView& stream) {
        return std::make_unique<HostByteVector>(
            cudf::io::write_parquet(options.inner, stream.inner));
    }
} // namespace libcudf_bridge
