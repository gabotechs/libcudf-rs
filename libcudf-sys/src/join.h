#pragma once

#include <memory>
#include "rust/cxx.h"
#include "table.h"
#include "column.h"
#include <cudf/join/hash_join.hpp>

namespace libcudf_bridge {
    struct HashJoin {
        std::unique_ptr<cudf::hash_join> inner;

        HashJoin();

        ~HashJoin();
    };

    std::unique_ptr<HashJoin> hash_join_create(
        const TableView& build_keys, bool nulls_equal);

    std::unique_ptr<Table> hash_join_inner_join_gather(
        const HashJoin& join,
        const TableView& probe_keys,
        const TableView& build_payload,
        const TableView& probe_payload);

    std::unique_ptr<Table> inner_join_gather(
        const TableView& left_keys,  const TableView& right_keys,
        const TableView& left_payload, const TableView& right_payload);

    std::unique_ptr<Table> left_join_gather(
        const TableView& left_keys,  const TableView& right_keys,
        const TableView& left_payload, const TableView& right_payload);

    std::unique_ptr<Table> full_join_gather(
        const TableView& left_keys,  const TableView& right_keys,
        const TableView& left_payload, const TableView& right_payload);

    std::unique_ptr<Table> left_semi_join_gather(
        const TableView& left_keys, const TableView& right_keys,
        const TableView& left_payload);

    std::unique_ptr<Table> left_anti_join_gather(
        const TableView& left_keys, const TableView& right_keys,
        const TableView& left_payload);

    std::unique_ptr<Table> cross_join(const TableView& left, const TableView& right);
} // namespace libcudf_bridge
