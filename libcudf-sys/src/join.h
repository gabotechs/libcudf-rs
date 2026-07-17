#pragma once

#include <memory>
#include <cstdint>
#include "rust/cxx.h"
#include "ast.h"
#include "memory_resource.h"
#include "stream.h"
#include "table.h"
#include "column.h"
#include <cudf/join/filtered_join.hpp>
#include <cudf/join/hash_join.hpp>
#include <cudf/join/join.hpp>
#include <cudf/column/column.hpp>
#include <rmm/device_uvector.hpp>
#include <vector>

namespace libcudf_bridge {
    struct DeviceIndexVector {
        std::unique_ptr<rmm::device_uvector<cudf::size_type>> inner;

        DeviceIndexVector();

        ~DeviceIndexVector();

        [[nodiscard]] size_t size() const;

        [[nodiscard]] std::unique_ptr<ColumnView> view() const;
    };

    struct JoinIndices {
        std::unique_ptr<DeviceIndexVector> left;
        std::unique_ptr<DeviceIndexVector> right;

        JoinIndices();

        ~JoinIndices();

        [[nodiscard]] std::unique_ptr<DeviceIndexVector> release_left();

        [[nodiscard]] std::unique_ptr<DeviceIndexVector> release_right();
    };

    struct HashJoinIndices {
        std::unique_ptr<DeviceIndexVector> probe;
        std::unique_ptr<DeviceIndexVector> build;

        HashJoinIndices();

        ~HashJoinIndices();

        [[nodiscard]] std::unique_ptr<DeviceIndexVector> release_probe();

        [[nodiscard]] std::unique_ptr<DeviceIndexVector> release_build();
    };

    struct HashJoin {
        std::unique_ptr<cudf::hash_join> inner;

        HashJoin();

        ~HashJoin();
    };

    struct FilteredJoin {
        std::unique_ptr<cudf::filtered_join> inner;

        FilteredJoin();

        ~FilteredJoin();
    };

    [[nodiscard]] int32_t join_no_match();

    [[nodiscard]] std::unique_ptr<HashJoin> hash_join_create(
        const TableView& build_keys,
        int32_t null_equality,
        const CudaStreamView& stream);

    [[nodiscard]] std::unique_ptr<HashJoinIndices> hash_join_inner_join_indices(
        const HashJoin& join,
        const TableView& probe_keys,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr);

    [[nodiscard]] std::unique_ptr<HashJoinIndices> hash_join_left_join_indices(
        const HashJoin& join,
        const TableView& probe_keys,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr);

    [[nodiscard]] std::unique_ptr<JoinIndices> inner_join_indices(
        const TableView& left_keys,
        const TableView& right_keys,
        int32_t null_equality,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr);

    [[nodiscard]] std::unique_ptr<JoinIndices> left_join_indices(
        const TableView& left_keys,
        const TableView& right_keys,
        int32_t null_equality,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr);

    [[nodiscard]] std::unique_ptr<JoinIndices> full_join_indices(
        const TableView& left_keys,
        const TableView& right_keys,
        int32_t null_equality,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr);

    [[nodiscard]] std::unique_ptr<JoinIndices> filter_join_indices(
        const TableView& left,
        const TableView& right,
        const DeviceIndexVector& left_indices,
        const DeviceIndexVector& right_indices,
        const AstExpressionTree& predicate,
        int32_t join_kind,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr);

    [[nodiscard]] std::unique_ptr<FilteredJoin> filtered_join_create(
        const TableView& build_keys,
        int32_t null_equality,
        int32_t set_as_build_table,
        const CudaStreamView& stream);

    [[nodiscard]] std::unique_ptr<DeviceIndexVector> filtered_join_semi_join(
        const FilteredJoin& join,
        const TableView& probe_keys,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr);

    [[nodiscard]] std::unique_ptr<DeviceIndexVector> filtered_join_anti_join(
        const FilteredJoin& join,
        const TableView& probe_keys,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr);

    [[nodiscard]] std::unique_ptr<Table> cross_join(
        const TableView& left,
        const TableView& right,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr);
} // namespace libcudf_bridge
