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
#include <cudf/groupby.hpp>

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

std::unique_ptr<TableView> Table::view() const {
    auto result = std::make_unique<TableView>();
    result->inner = std::make_unique<cudf::table_view>(inner->view());
    return result;
}

// TableView implementation
TableView::TableView() : inner(nullptr) {}

TableView::~TableView() = default;

size_t TableView::num_columns() const {
    return inner->num_columns();
}

size_t TableView::num_rows() const {
    return inner->num_rows();
}

std::unique_ptr<TableView> TableView::select(rust::Slice<const size_t> column_indices) const {
    std::vector<cudf::size_type> indices;
    indices.reserve(column_indices.size());
    for (auto idx : column_indices) {
        indices.push_back(static_cast<cudf::size_type>(idx));
    }

    auto result = std::make_unique<TableView>();
    result->inner = std::make_unique<cudf::table_view>(inner->select(indices));
    return result;
}

std::unique_ptr<ColumnView> TableView::column(size_t index) const {
    auto result = std::make_unique<ColumnView>();
    result->inner = std::make_unique<cudf::column_view>(inner->column(index));
    return result;
}

// ColumnView implementation
ColumnView::ColumnView() : inner(nullptr) {}

ColumnView::~ColumnView() = default;

size_t ColumnView::size() const {
    return inner->size();
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

// Scalar implementation
Scalar::Scalar() : inner(nullptr) {}

Scalar::~Scalar() = default;

bool Scalar::is_valid() const {
    if (!inner) {
        return false;
    }
    return inner->is_valid();
}

// Aggregation implementation
Aggregation::Aggregation() : inner(nullptr) {}

Aggregation::~Aggregation() = default;

// GroupBy implementation
GroupBy::GroupBy() : inner(nullptr) {}

GroupBy::~GroupBy() = default;

// TODO: this is big, there are clones... I'm not sure if this is right.
std::unique_ptr<GroupByResult> GroupBy::aggregate(rust::Slice<const AggregationRequest* const> requests) const {
    std::vector<cudf::groupby::aggregation_request> cudf_requests;
    cudf_requests.reserve(requests.size());
    for (auto* req : requests) {
        cudf::groupby::aggregation_request cudf_req;
        cudf_req.values = req->inner->values;
        for (auto& agg : req->inner->aggregations) {
            auto cloned = agg->clone();
            auto* groupby_agg = dynamic_cast<cudf::groupby_aggregation*>(cloned.release());
            cudf_req.aggregations.push_back(std::unique_ptr<cudf::groupby_aggregation>(groupby_agg));
        }
        cudf_requests.push_back(std::move(cudf_req));
    }

    auto result = inner->aggregate(cudf_requests);

    auto wrapped = std::make_unique<GroupByResult>();
    wrapped->keys.inner = std::move(result.first);

    for (auto& agg_result : result.second) {
        AggregationResult ar;
        for (auto& col : agg_result.results) {
            Column wrapped_col;
            wrapped_col.inner = std::move(col);
            ar.results.emplace_back(std::move(wrapped_col));
        }
        wrapped->results.push_back(std::move(ar));
    }

    return wrapped;
}

// AggregationRequest implementation
AggregationRequest::AggregationRequest() : inner(std::make_unique<cudf::groupby::aggregation_request>()) {}

AggregationRequest::~AggregationRequest() = default;

void AggregationRequest::add(std::unique_ptr<Aggregation> agg) {
    auto* groupby_agg = dynamic_cast<cudf::groupby_aggregation*>(agg->inner.release());
    inner->aggregations.push_back(std::unique_ptr<cudf::groupby_aggregation>(groupby_agg));
}

// AggregationResult implementation
AggregationResult::AggregationResult() = default;

AggregationResult::~AggregationResult() = default;

size_t AggregationResult::len() const {
    return results.size();
}

const Column& AggregationResult::get(size_t index) const {
    return results[index];
}

// GroupByResult implementation
GroupByResult::GroupByResult() = default;

GroupByResult::~GroupByResult() = default;

const Table& GroupByResult::get_keys() const {
    return keys;
}

size_t GroupByResult::results_size() const {
    return results.size();
}

const AggregationResult& GroupByResult::get_result(size_t index) const {
    return results[index];
}

AggregationResult& GroupByResult::get_result_mut(size_t index) {
    return results[index];
}

// Factory functions
std::unique_ptr<Table> create_empty_table() {
    auto table = std::make_unique<Table>();
    std::vector<std::unique_ptr<cudf::column>> columns;
    table->inner = std::make_unique<cudf::table>(std::move(columns));
    return table;
}

std::unique_ptr<Table> create_table_from_columns(rust::Slice<Column* const> columns) {
    std::vector<std::unique_ptr<cudf::column>> cudf_columns;
    cudf_columns.reserve(columns.size());

    for (auto* col : columns) {
        cudf_columns.push_back(std::move(col->inner));
    }

    auto table = std::make_unique<Table>();
    table->inner = std::make_unique<cudf::table>(std::move(cudf_columns));
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

// Aggregation factory functions - direct cuDF mappings
std::unique_ptr<Aggregation> make_sum_aggregation() {
    auto result = std::make_unique<Aggregation>();
    result->inner = cudf::make_sum_aggregation<cudf::reduce_aggregation>();
    return result;
}

std::unique_ptr<Aggregation> make_min_aggregation() {
    auto result = std::make_unique<Aggregation>();
    result->inner = cudf::make_min_aggregation<cudf::reduce_aggregation>();
    return result;
}

std::unique_ptr<Aggregation> make_max_aggregation() {
    auto result = std::make_unique<Aggregation>();
    result->inner = cudf::make_max_aggregation<cudf::reduce_aggregation>();
    return result;
}

std::unique_ptr<Aggregation> make_mean_aggregation() {
    auto result = std::make_unique<Aggregation>();
    result->inner = cudf::make_mean_aggregation<cudf::reduce_aggregation>();
    return result;
}

std::unique_ptr<Aggregation> make_count_aggregation() {
    auto result = std::make_unique<Aggregation>();
    result->inner = cudf::make_count_aggregation<cudf::reduce_aggregation>();
    return result;
}

// Aggregation factory functions - direct cuDF mappings (for groupby)
std::unique_ptr<Aggregation> make_sum_aggregation_groupby() {
    auto result = std::make_unique<Aggregation>();
    result->inner = cudf::make_sum_aggregation<cudf::groupby_aggregation>();
    return result;
}

std::unique_ptr<Aggregation> make_min_aggregation_groupby() {
    auto result = std::make_unique<Aggregation>();
    result->inner = cudf::make_min_aggregation<cudf::groupby_aggregation>();
    return result;
}

std::unique_ptr<Aggregation> make_max_aggregation_groupby() {
    auto result = std::make_unique<Aggregation>();
    result->inner = cudf::make_max_aggregation<cudf::groupby_aggregation>();
    return result;
}

std::unique_ptr<Aggregation> make_mean_aggregation_groupby() {
    auto result = std::make_unique<Aggregation>();
    result->inner = cudf::make_mean_aggregation<cudf::groupby_aggregation>();
    return result;
}

std::unique_ptr<Aggregation> make_count_aggregation_groupby() {
    auto result = std::make_unique<Aggregation>();
    result->inner = cudf::make_count_aggregation<cudf::groupby_aggregation>();
    return result;
}

// Reduction - direct cuDF mapping
std::unique_ptr<Scalar> reduce(const Column& col, const Aggregation& agg, int32_t output_type_id) {
    auto result = std::make_unique<Scalar>();
    auto output_type = cudf::data_type{static_cast<cudf::type_id>(output_type_id)};
    auto* reduce_agg = dynamic_cast<cudf::reduce_aggregation const*>(agg.inner.get());
    result->inner = cudf::reduce(col.inner->view(), *reduce_agg, output_type);
    return result;
}

// GroupBy operations - direct cuDF mappings
std::unique_ptr<GroupBy> groupby_create(const TableView& keys) {
    auto result = std::make_unique<GroupBy>();
    result->inner = std::make_unique<cudf::groupby::groupby>(*keys.inner);
    return result;
}

std::unique_ptr<AggregationRequest> aggregation_request_create(const ColumnView& values) {
    auto result = std::make_unique<AggregationRequest>();
    result->inner->values = *values.inner;
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
