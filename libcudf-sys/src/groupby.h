#pragma once

#include <cstdint>
#include <memory>
#include <vector>
#include "rust/cxx.h"
#include "table.h"
#include "column.h"
#include "aggregation.h"
#include "stream.h"
#include "memory_resource.h"
#include <cudf/groupby.hpp>

namespace libcudf_bridge {
    // Helper to extract a column from a vector by moving it
    struct ColumnVectorHelper {
        std::vector<Column> columns;

        ColumnVectorHelper();
        ~ColumnVectorHelper();

        [[nodiscard]] size_t len() const;
        [[nodiscard]] bool is_empty() const;
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

        [[nodiscard]] bool is_empty() const;

        [[nodiscard]] std::unique_ptr<ColumnVectorHelper> release_result(size_t index);
    };

    // Opaque wrapper for cuDF aggregation_request
    struct AggregationRequest {
        std::unique_ptr<cudf::groupby::aggregation_request> inner;

        AggregationRequest();

        ~AggregationRequest();

        // Direct cuDF method (adds aggregation to the request)
        void add(std::unique_ptr<GroupByAggregation> agg) const;
    };

    // FFI storage for the contiguous host_span expected by groupby::aggregate
    struct AggregationRequests {
        std::vector<cudf::groupby::aggregation_request> inner;

        AggregationRequests();

        ~AggregationRequests();

        void add(std::unique_ptr<AggregationRequest> request);
    };

    // Opaque wrapper for cuDF groupby
    struct GroupBy {
        std::unique_ptr<cudf::groupby::groupby> inner;

        GroupBy();

        ~GroupBy();

        // Direct cuDF method
        [[nodiscard]] std::unique_ptr<GroupByResult> aggregate(
            const AggregationRequests &requests,
            const CudaStreamView &stream,
            const DeviceAsyncResourceRef &mr) const;
    };

    // GroupBy operations - direct cuDF mappings
    std::unique_ptr<GroupBy> groupby_create(
        const TableView &keys,
        int32_t null_handling,
        int32_t keys_are_sorted,
        rust::Slice<const int32_t> column_order,
        rust::Slice<const int32_t> null_precedence);

    std::unique_ptr<AggregationRequest> aggregation_request_create(const ColumnView &values);

    std::unique_ptr<AggregationRequests> aggregation_requests_create();
} // namespace libcudf_bridge
