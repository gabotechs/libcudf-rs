#pragma once

#include <memory>
#include <vector>
#include "rust/cxx.h"
#include "table.h"
#include "column.h"
#include "aggregation.h"
#include <cudf/groupby.hpp>

namespace libcudf_bridge {
    // Helper to extract a column from a vector by moving it
    struct ColumnVectorHelper {
        std::vector<Column> columns;

        ColumnVectorHelper();
        ~ColumnVectorHelper();

        [[nodiscard]] size_t len() const;
        [[nodiscard]] std::unique_ptr<Column> release(size_t index);
    };

    // Direct exposure of cuDF's groupby aggregate() return type
    struct GroupByResult {
        Table keys;
        std::vector<std::vector<Column> > results;

        GroupByResult();

        ~GroupByResult();

        [[nodiscard]] std::unique_ptr<Table> release_keys();

        [[nodiscard]] size_t len() const;

        [[nodiscard]] std::unique_ptr<ColumnVectorHelper> release_result(size_t index);
    };

    // Opaque wrapper for cuDF aggregation_request
    struct AggregationRequest {
        std::unique_ptr<cudf::groupby::aggregation_request> inner;

        AggregationRequest();

        ~AggregationRequest();

        // Direct cuDF method (adds aggregation to the request)
        void add(std::unique_ptr<Aggregation> agg) const;
    };

    // Opaque wrapper for cuDF groupby
    struct GroupBy {
        std::unique_ptr<cudf::groupby::groupby> inner;

        GroupBy();

        ~GroupBy();

        // Direct cuDF method
        [[nodiscard]] std::unique_ptr<GroupByResult> aggregate(
            rust::Slice<const AggregationRequest * const> requests) const;
    };

    // GroupBy operations - direct cuDF mappings
    std::unique_ptr<GroupBy> groupby_create(const TableView &keys);

    std::unique_ptr<AggregationRequest> aggregation_request_create(const ColumnView &values);
} // namespace libcudf_bridge
