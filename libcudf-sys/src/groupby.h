#pragma once

#include <memory>
#include <vector>
#include "rust/cxx.h"
#include "table.h"
#include "column.h"
#include "aggregation.h"

// Forward declarations of cuDF types
namespace cudf {
    namespace groupby {
        class groupby;
        struct aggregation_request;
    }
}

namespace libcudf_bridge {

    // Direct exposure of cuDF's groupby aggregate() return type
    struct GroupByResult {
        Table keys;
        std::vector<ColumnCollection> results;

        GroupByResult();
        ~GroupByResult();

        // Field accessors as methods
        [[nodiscard]] const Table &get_keys() const;
        [[nodiscard]] std::unique_ptr<Table> release_keys();
        [[nodiscard]] size_t len() const;
        [[nodiscard]] const ColumnCollection &get(size_t index) const;
        [[nodiscard]] ColumnCollection &get_mut(size_t index);
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
