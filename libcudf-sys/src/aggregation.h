#pragma once

#include <cstdint>
#include <memory>
#include "rust/cxx.h"
#include "column.h"
#include "data_type.h"
#include "scalar.h"
#include "stream.h"
#include "memory_resource.h"
#include <cudf/aggregation.hpp>

namespace libcudf_bridge {

    // Opaque wrapper for cuDF reduce_aggregation
    struct ReduceAggregation {
        std::unique_ptr<cudf::reduce_aggregation> inner;

        ReduceAggregation();
        ~ReduceAggregation();
    };

    // Opaque wrapper for cuDF groupby_aggregation
    struct GroupByAggregation {
        std::unique_ptr<cudf::groupby_aggregation> inner;

        GroupByAggregation();
        ~GroupByAggregation();
    };

    // Aggregation factory functions - direct cuDF mappings (for reduce)
    std::unique_ptr<ReduceAggregation> make_sum_aggregation();
    std::unique_ptr<ReduceAggregation> make_min_aggregation();
    std::unique_ptr<ReduceAggregation> make_max_aggregation();
    std::unique_ptr<ReduceAggregation> make_mean_aggregation();
    std::unique_ptr<ReduceAggregation> make_count_aggregation(int32_t null_handling);
    std::unique_ptr<ReduceAggregation> make_variance_aggregation(int32_t ddof);
    std::unique_ptr<ReduceAggregation> make_std_aggregation(int32_t ddof);
    std::unique_ptr<ReduceAggregation> make_nunique_aggregation(int32_t null_handling);
    std::unique_ptr<ReduceAggregation> make_median_aggregation();

    // Aggregation factory functions - direct cuDF mappings (for groupby)
    std::unique_ptr<GroupByAggregation> make_sum_aggregation_groupby();
    std::unique_ptr<GroupByAggregation> make_min_aggregation_groupby();
    std::unique_ptr<GroupByAggregation> make_max_aggregation_groupby();
    std::unique_ptr<GroupByAggregation> make_mean_aggregation_groupby();
    std::unique_ptr<GroupByAggregation> make_count_aggregation_groupby(int32_t null_handling);
    std::unique_ptr<GroupByAggregation> make_variance_aggregation_groupby(int32_t ddof);
    std::unique_ptr<GroupByAggregation> make_std_aggregation_groupby(int32_t ddof);
    std::unique_ptr<GroupByAggregation> make_nunique_aggregation_groupby(int32_t null_handling);
    std::unique_ptr<GroupByAggregation> make_median_aggregation_groupby();

    // Reduction - direct cuDF mapping
    std::unique_ptr<Scalar> reduce(
        const ColumnView &col,
        const ReduceAggregation &agg,
        const DataType &output_type,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr);

    std::unique_ptr<Scalar> reduce_with_init(
        const ColumnView &col,
        const ReduceAggregation &agg,
        const DataType &output_type,
        const Scalar &init,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr);
} // namespace libcudf_bridge
