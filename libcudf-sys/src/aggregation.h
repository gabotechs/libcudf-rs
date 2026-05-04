#pragma once

#include <cstdint>
#include <memory>
#include "rust/cxx.h"
#include "column.h"
#include "scalar.h"
#include <cudf/aggregation.hpp>

namespace libcudf_bridge {

    // Opaque wrapper for cuDF aggregation
    struct Aggregation {
        std::unique_ptr<cudf::aggregation> inner;

        Aggregation();
        ~Aggregation();
    };

    // Aggregation factory functions - direct cuDF mappings (for reduce)
    std::unique_ptr<Aggregation> make_sum_aggregation();
    std::unique_ptr<Aggregation> make_min_aggregation();
    std::unique_ptr<Aggregation> make_max_aggregation();
    std::unique_ptr<Aggregation> make_mean_aggregation();
    std::unique_ptr<Aggregation> make_count_aggregation();
    std::unique_ptr<Aggregation> make_variance_aggregation(int32_t ddof);
    std::unique_ptr<Aggregation> make_std_aggregation(int32_t ddof);
    std::unique_ptr<Aggregation> make_nunique_aggregation();
    std::unique_ptr<Aggregation> make_median_aggregation();

    // Aggregation factory functions - direct cuDF mappings (for groupby)
    std::unique_ptr<Aggregation> make_sum_aggregation_groupby();
    std::unique_ptr<Aggregation> make_min_aggregation_groupby();
    std::unique_ptr<Aggregation> make_max_aggregation_groupby();
    std::unique_ptr<Aggregation> make_mean_aggregation_groupby();
    std::unique_ptr<Aggregation> make_count_aggregation_groupby();
    std::unique_ptr<Aggregation> make_variance_aggregation_groupby(int32_t ddof);
    std::unique_ptr<Aggregation> make_std_aggregation_groupby(int32_t ddof);
    std::unique_ptr<Aggregation> make_nunique_aggregation_groupby();
    std::unique_ptr<Aggregation> make_median_aggregation_groupby();

    // Reduction - direct cuDF mapping
    std::unique_ptr<Scalar> reduce(const Column &col, const Aggregation &agg, int32_t output_type_id);
} // namespace libcudf_bridge
