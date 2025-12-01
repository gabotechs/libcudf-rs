#include "groupby.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/table/table.hpp>
#include <cudf/groupby.hpp>
#include <cudf/aggregation.hpp>

namespace libcudf_bridge {
    // Helper function to create AggregationResult from cudf::groupby::aggregation_result
    AggregationResult aggregation_result_from_cudf(cudf::groupby::aggregation_result cudf_result) {
        AggregationResult ar;
        for (auto &cudf_col: cudf_result.results) {
            auto col = column_from_unique_ptr(std::move(cudf_col));
            ar.results.emplace_back(std::move(col));
        }
        return ar;
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
            auto ar = aggregation_result_from_cudf(std::move(cudf_agg_result));
            group_by_result->results.push_back(std::move(ar));
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

    // AggregationResult implementation
    AggregationResult::AggregationResult() = default;

    AggregationResult::~AggregationResult() = default;

    size_t AggregationResult::len() const {
        return results.size();
    }

    const Column &AggregationResult::get(size_t index) const {
        return results[index];
    }

    // GroupByResult implementation
    GroupByResult::GroupByResult() = default;

    GroupByResult::~GroupByResult() = default;

    const Table &GroupByResult::get_keys() const {
        return keys;
    }

    size_t GroupByResult::results_size() const {
        return results.size();
    }

    const AggregationResult &GroupByResult::get_result(size_t index) const {
        return results[index];
    }

    AggregationResult &GroupByResult::get_result_mut(size_t index) {
        return results[index];
    }
} // namespace libcudf_bridge
