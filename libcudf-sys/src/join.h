#pragma once

#include <memory>
#include <cstdint>
#include "rust/cxx.h"
#include "table.h"
#include "column.h"
#include <cudf/join/hash_join.hpp>
#include <cudf/column/column.hpp>
#include <vector>

namespace libcudf_bridge {
    struct JoinIndices {
        std::unique_ptr<Column> left;
        std::unique_ptr<Column> right;

        JoinIndices();

        ~JoinIndices();

        std::unique_ptr<Column> release_left();

        std::unique_ptr<Column> release_right();
    };

    struct HashJoinIndices {
        std::unique_ptr<Column> probe;
        std::unique_ptr<Column> build;

        HashJoinIndices();

        ~HashJoinIndices();

        std::unique_ptr<Column> release_probe();

        std::unique_ptr<Column> release_build();
    };

    struct HashJoin {
        std::unique_ptr<cudf::hash_join> inner;

        HashJoin();

        ~HashJoin();
    };

    std::unique_ptr<HashJoin> hash_join_create(
        const TableView& build_keys, int32_t null_equality);

    std::unique_ptr<HashJoinIndices> hash_join_inner_join_indices(
        const HashJoin& join,
        const TableView& probe_keys);

    std::unique_ptr<HashJoinIndices> hash_join_left_join_indices(
        const HashJoin& join,
        const TableView& probe_keys);

    std::unique_ptr<JoinIndices> inner_join_indices(
        const TableView& left_keys,
        const TableView& right_keys);

    std::unique_ptr<JoinIndices> left_join_indices(
        const TableView& left_keys,
        const TableView& right_keys);

    std::unique_ptr<JoinIndices> full_join_indices(
        const TableView& left_keys,
        const TableView& right_keys);

    std::unique_ptr<Column> left_semi_join_indices(
        const TableView& left_keys,
        const TableView& right_keys);

    std::unique_ptr<Column> left_anti_join_indices(
        const TableView& left_keys,
        const TableView& right_keys);

    std::unique_ptr<Table> cross_join(const TableView& left, const TableView& right);
} // namespace libcudf_bridge
