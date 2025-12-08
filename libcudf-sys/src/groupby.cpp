#include "groupby.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/table/table.hpp>
#include <cudf/groupby.hpp>
#include <cudf/aggregation.hpp>

namespace libcudf_bridge {
    // ColumnVectorHelper implementation
    ColumnVectorHelper::ColumnVectorHelper() = default;

    ColumnVectorHelper::~ColumnVectorHelper() = default;

    size_t ColumnVectorHelper::len() const {
        return columns.size();
    }

    std::unique_ptr<Column> ColumnVectorHelper::release(size_t index) {
        if (index >= columns.size()) {
            throw std::out_of_range("Index out of bounds");
        }
        return std::make_unique<Column>(std::move(columns[index]));
    }

    // Aggregation implementation
    Aggregation::Aggregation() : inner(nullptr) {
    }

    Aggregation::~Aggregation() = default;

    // GroupBy implementation
    GroupBy::GroupBy() : inner(nullptr) {
    }

    GroupBy::~GroupBy() = default;

    // TODO: this is big, there are clones... I'm not sure if this is right.
    std::unique_ptr<GroupByResult> GroupBy::aggregate(rust::Slice<const AggregationRequest * const> requests) const {
        std::vector<cudf::groupby::aggregation_request> cudf_requests;
        cudf_requests.reserve(requests.size());
        for (auto *req: requests) {
            cudf::groupby::aggregation_request cudf_req;
            cudf_req.values = req->inner->values;
            for (auto &agg: req->inner->aggregations) {
                auto cloned = agg->clone();
                auto *groupby_agg = dynamic_cast<cudf::groupby_aggregation *>(cloned.release());
                cudf_req.aggregations.push_back(std::unique_ptr<cudf::groupby_aggregation>(groupby_agg));
            }
            cudf_requests.push_back(std::move(cudf_req));
        }

        auto aggregate_result = inner->aggregate(cudf_requests);

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

    void AggregationRequest::add(std::unique_ptr<Aggregation> agg) const {
        auto *groupby_agg = dynamic_cast<cudf::groupby_aggregation *>(agg->inner.release());
        inner->aggregations.push_back(std::unique_ptr<cudf::groupby_aggregation>(groupby_agg));
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

    std::unique_ptr<ColumnVectorHelper> GroupByResult::release_result(size_t index) {
        if (index >= results.size()) {
            throw std::out_of_range("Index out of bounds");
        }
        auto helper = std::make_unique<ColumnVectorHelper>();
        helper->columns = std::move(results[index]);
        return helper;
    }
} // namespace libcudf_bridge
