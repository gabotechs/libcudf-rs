#include "aggregation.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/table/table_view.hpp>
#include <cudf/column/column.hpp>
#include <cudf/aggregation.hpp>
#include <cudf/reduction.hpp>
#include <cudf/groupby.hpp>

namespace libcudf_bridge {
    // Aggregation factory functions - direct cuDF mappings (for reduce)
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
    std::unique_ptr<Scalar> reduce(const Column &col, const Aggregation &agg, int32_t output_type_id) {
        auto result = std::make_unique<Scalar>();
        const auto output_type = cudf::data_type{static_cast<cudf::type_id>(output_type_id)};
        auto *reduce_agg = dynamic_cast<cudf::reduce_aggregation const *>(agg.inner.get());
        result->inner = cudf::reduce(col.inner->view(), *reduce_agg, output_type);
        return result;
    }

    // GroupBy factory
    std::unique_ptr<GroupBy> groupby_create(const TableView &keys) {
        auto result = std::make_unique<GroupBy>();
        result->inner = std::make_unique<cudf::groupby::groupby>(*keys.inner);
        return result;
    }

    // AggregationRequest factory
    std::unique_ptr<AggregationRequest> aggregation_request_create(const ColumnView &values) {
        auto result = std::make_unique<AggregationRequest>();
        result->inner->values = *values.inner;
        return result;
    }
} // namespace libcudf_bridge
