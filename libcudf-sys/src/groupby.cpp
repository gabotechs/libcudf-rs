#include "groupby.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/table/table.hpp>
#include <cudf/groupby.hpp>
#include <cudf/aggregation.hpp>
#include <cudf/types.hpp>

namespace libcudf_bridge {
    // ColumnVectorHelper implementation
    ColumnVectorHelper::ColumnVectorHelper() = default;

    ColumnVectorHelper::~ColumnVectorHelper() = default;

    size_t ColumnVectorHelper::len() const {
        return columns.size();
    }

    bool ColumnVectorHelper::is_empty() const {
        return columns.empty();
    }

    std::unique_ptr<Column> ColumnVectorHelper::release(size_t index) {
        if (index >= columns.size()) {
            throw std::out_of_range("Index out of bounds");
        }
        return std::make_unique<Column>(std::move(columns[index]));
    }

    // GroupBy implementation
    GroupBy::GroupBy() : inner(nullptr) {
    }

    GroupBy::~GroupBy() = default;

    std::unique_ptr<GroupByResult> GroupBy::aggregate(const AggregationRequests &requests) const {
        auto aggregate_result = inner->aggregate(requests.inner);

        auto group_by_result = std::make_unique<GroupByResult>();
        group_by_result->keys.inner = std::move(aggregate_result.first);

        for (auto &cudf_agg_result: aggregate_result.second) {
            auto result = std::vector<Column>();
            result.reserve(cudf_agg_result.results.size());
            for (auto &col: cudf_agg_result.results) {
                result.emplace_back(column_from_unique_ptr(std::move(col)));
            }
            group_by_result->results.emplace_back(std::move(result));
        }

        return group_by_result;
    }

    std::unique_ptr<GroupByResult> GroupBy::aggregate_on(
        const AggregationRequests &requests,
        const CudaStream &stream) const {
        auto aggregate_result = inner->aggregate(requests.inner, stream.view());

        auto group_by_result = std::make_unique<GroupByResult>();
        group_by_result->keys.inner = std::move(aggregate_result.first);

        for (auto &cudf_agg_result: aggregate_result.second) {
            auto result = std::vector<Column>();
            result.reserve(cudf_agg_result.results.size());
            for (auto &col: cudf_agg_result.results) {
                result.emplace_back(column_from_unique_ptr(std::move(col)));
            }
            group_by_result->results.emplace_back(std::move(result));
        }

        return group_by_result;
    }

    // AggregationRequest implementation
    AggregationRequest::AggregationRequest() : inner(std::make_unique<cudf::groupby::aggregation_request>()) {
    }

    AggregationRequest::~AggregationRequest() = default;

    void AggregationRequest::add(std::unique_ptr<GroupByAggregation> agg) const {
        inner->aggregations.push_back(std::move(agg->inner));
    }

    AggregationRequests::AggregationRequests() = default;

    AggregationRequests::~AggregationRequests() = default;

    void AggregationRequests::add(std::unique_ptr<AggregationRequest> request) {
        inner.push_back(std::move(*request->inner));
    }

    // GroupByResult implementation
    GroupByResult::GroupByResult() = default;

    GroupByResult::~GroupByResult() = default;

    std::unique_ptr<Table> GroupByResult::release_keys() {
        auto table = std::make_unique<Table>();
        table->inner = std::move(keys.inner);
        return table;
    }

    size_t GroupByResult::len() const {
        return results.size();
    }

    bool GroupByResult::is_empty() const {
        return results.empty();
    }

    std::unique_ptr<ColumnVectorHelper> GroupByResult::release_result(size_t index) {
        if (index >= results.size()) {
            throw std::out_of_range("Index out of bounds");
        }
        auto helper = std::make_unique<ColumnVectorHelper>();
        helper->columns = std::move(results[index]);
        return helper;
    }

    std::unique_ptr<GroupBy> groupby_create(
        const TableView &keys,
        int32_t null_handling,
        int32_t keys_are_sorted,
        rust::Slice<const int32_t> column_order,
        rust::Slice<const int32_t> null_precedence) {
        std::vector<cudf::order> orders;
        orders.reserve(column_order.size());
        for (auto ord: column_order) {
            orders.push_back(static_cast<cudf::order>(ord));
        }

        std::vector<cudf::null_order> null_orders;
        null_orders.reserve(null_precedence.size());
        for (auto null_ord: null_precedence) {
            null_orders.push_back(static_cast<cudf::null_order>(null_ord));
        }

        auto result = std::make_unique<GroupBy>();
        result->inner = std::make_unique<cudf::groupby::groupby>(
            *keys.inner,
            static_cast<cudf::null_policy>(null_handling),
            static_cast<cudf::sorted>(keys_are_sorted),
            orders,
            null_orders);
        return result;
    }

    std::unique_ptr<AggregationRequest> aggregation_request_create(const ColumnView &values) {
        auto result = std::make_unique<AggregationRequest>();
        result->inner->values = *values.inner;
        return result;
    }

    std::unique_ptr<AggregationRequests> aggregation_requests_create() {
        return std::make_unique<AggregationRequests>();
    }
} // namespace libcudf_bridge
