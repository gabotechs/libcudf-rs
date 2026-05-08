#include "aggregation.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/table/table_view.hpp>
#include <cudf/column/column.hpp>
#include <cudf/aggregation.hpp>
#include <cudf/reduction.hpp>
#include <cudf/groupby.hpp>

#include <functional>
#include <stdexcept>

namespace libcudf_bridge {
    ReduceAggregation::ReduceAggregation() : inner(nullptr) {
    }

    ReduceAggregation::~ReduceAggregation() = default;

    GroupByAggregation::GroupByAggregation() : inner(nullptr) {
    }

    GroupByAggregation::~GroupByAggregation() = default;

    // Aggregation factory functions - direct cuDF mappings (for reduce)
    std::unique_ptr<ReduceAggregation> make_sum_aggregation() {
        auto result = std::make_unique<ReduceAggregation>();
        result->inner = cudf::make_sum_aggregation<cudf::reduce_aggregation>();
        return result;
    }

    std::unique_ptr<ReduceAggregation> make_min_aggregation() {
        auto result = std::make_unique<ReduceAggregation>();
        result->inner = cudf::make_min_aggregation<cudf::reduce_aggregation>();
        return result;
    }

    std::unique_ptr<ReduceAggregation> make_max_aggregation() {
        auto result = std::make_unique<ReduceAggregation>();
        result->inner = cudf::make_max_aggregation<cudf::reduce_aggregation>();
        return result;
    }

    std::unique_ptr<ReduceAggregation> make_mean_aggregation() {
        auto result = std::make_unique<ReduceAggregation>();
        result->inner = cudf::make_mean_aggregation<cudf::reduce_aggregation>();
        return result;
    }

    std::unique_ptr<ReduceAggregation> make_count_aggregation(int32_t null_handling) {
        auto result = std::make_unique<ReduceAggregation>();
        result->inner = cudf::make_count_aggregation<cudf::reduce_aggregation>(
            static_cast<cudf::null_policy>(null_handling));
        return result;
    }

    std::unique_ptr<ReduceAggregation> make_variance_aggregation(int32_t ddof) {
        auto result = std::make_unique<ReduceAggregation>();
        result->inner = cudf::make_variance_aggregation<cudf::reduce_aggregation>(ddof);
        return result;
    }

    std::unique_ptr<ReduceAggregation> make_std_aggregation(int32_t ddof) {
        auto result = std::make_unique<ReduceAggregation>();
        result->inner = cudf::make_std_aggregation<cudf::reduce_aggregation>(ddof);
        return result;
    }

    std::unique_ptr<ReduceAggregation> make_nunique_aggregation(int32_t null_handling) {
        auto result = std::make_unique<ReduceAggregation>();
        result->inner = cudf::make_nunique_aggregation<cudf::reduce_aggregation>(
            static_cast<cudf::null_policy>(null_handling));
        return result;
    }

    std::unique_ptr<ReduceAggregation> make_median_aggregation() {
        auto result = std::make_unique<ReduceAggregation>();
        result->inner = cudf::make_median_aggregation<cudf::reduce_aggregation>();
        return result;
    }

    // Aggregation factory functions - direct cuDF mappings (for groupby)
    std::unique_ptr<GroupByAggregation> make_sum_aggregation_groupby() {
        auto result = std::make_unique<GroupByAggregation>();
        result->inner = cudf::make_sum_aggregation<cudf::groupby_aggregation>();
        return result;
    }

    std::unique_ptr<GroupByAggregation> make_min_aggregation_groupby() {
        auto result = std::make_unique<GroupByAggregation>();
        result->inner = cudf::make_min_aggregation<cudf::groupby_aggregation>();
        return result;
    }

    std::unique_ptr<GroupByAggregation> make_max_aggregation_groupby() {
        auto result = std::make_unique<GroupByAggregation>();
        result->inner = cudf::make_max_aggregation<cudf::groupby_aggregation>();
        return result;
    }

    std::unique_ptr<GroupByAggregation> make_mean_aggregation_groupby() {
        auto result = std::make_unique<GroupByAggregation>();
        result->inner = cudf::make_mean_aggregation<cudf::groupby_aggregation>();
        return result;
    }

    std::unique_ptr<GroupByAggregation> make_count_aggregation_groupby(int32_t null_handling) {
        auto result = std::make_unique<GroupByAggregation>();
        result->inner = cudf::make_count_aggregation<cudf::groupby_aggregation>(
            static_cast<cudf::null_policy>(null_handling));
        return result;
    }

    std::unique_ptr<GroupByAggregation> make_variance_aggregation_groupby(int32_t ddof) {
        auto result = std::make_unique<GroupByAggregation>();
        result->inner = cudf::make_variance_aggregation<cudf::groupby_aggregation>(ddof);
        return result;
    }

    std::unique_ptr<GroupByAggregation> make_std_aggregation_groupby(int32_t ddof) {
        auto result = std::make_unique<GroupByAggregation>();
        result->inner = cudf::make_std_aggregation<cudf::groupby_aggregation>(ddof);
        return result;
    }

    std::unique_ptr<GroupByAggregation> make_nunique_aggregation_groupby(int32_t null_handling) {
        auto result = std::make_unique<GroupByAggregation>();
        result->inner = cudf::make_nunique_aggregation<cudf::groupby_aggregation>(
            static_cast<cudf::null_policy>(null_handling));
        return result;
    }

    std::unique_ptr<GroupByAggregation> make_median_aggregation_groupby() {
        auto result = std::make_unique<GroupByAggregation>();
        result->inner = cudf::make_median_aggregation<cudf::groupby_aggregation>();
        return result;
    }


    // Reduction - direct cuDF mapping
    std::unique_ptr<Scalar> reduce(
        const ColumnView &col,
        const ReduceAggregation &agg,
        const DataType &output_type,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr) {
        auto result = std::make_unique<Scalar>();
        result->inner = cudf::reduce(*col.inner, *agg.inner, output_type.inner, stream.inner, mr.inner);
        return result;
    }

    std::unique_ptr<Scalar> reduce_with_init(
        const ColumnView &col,
        const ReduceAggregation &agg,
        const DataType &output_type,
        const Scalar &init,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr) {
        auto result = std::make_unique<Scalar>();
        result->inner = cudf::reduce(
            *col.inner,
            *agg.inner,
            output_type.inner,
            std::cref(*init.inner),
            stream.inner,
            mr.inner);
        return result;
    }
} // namespace libcudf_bridge
