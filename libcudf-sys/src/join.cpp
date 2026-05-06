#include "join.h"
#include <cudf/join/join.hpp>
#include <cudf/join/filtered_join.hpp>
#include <cudf/column/column.hpp>
#include <cudf/copying.hpp>

namespace libcudf_bridge {

static std::unique_ptr<cudf::column> uvector_to_column(
    std::unique_ptr<rmm::device_uvector<cudf::size_type>> vec)
{
    return std::make_unique<cudf::column>(std::move(*vec), rmm::device_buffer{}, 0);
}

// Gather left and right payloads with their respective maps and combine into one table.
static std::unique_ptr<Table> gather_combine(
    std::unique_ptr<rmm::device_uvector<cudf::size_type>> left_idx,
    std::unique_ptr<rmm::device_uvector<cudf::size_type>> right_idx,
    const cudf::table_view& left_payload,
    const cudf::table_view& right_payload,
    cudf::out_of_bounds_policy left_policy,
    cudf::out_of_bounds_policy right_policy)
{
    auto left_col  = uvector_to_column(std::move(left_idx));
    auto right_col = uvector_to_column(std::move(right_idx));

    auto left_result  = cudf::gather(left_payload,  left_col->view(),  left_policy);
    auto right_result = cudf::gather(right_payload, right_col->view(), right_policy);

    auto cols       = left_result->release();
    auto right_cols = right_result->release();
    cols.insert(cols.end(),
        std::make_move_iterator(right_cols.begin()),
        std::make_move_iterator(right_cols.end()));

    auto result = std::make_unique<Table>();
    result->inner = std::make_unique<cudf::table>(std::move(cols));
    return result;
}

std::unique_ptr<Table> inner_join_gather(
    const TableView& left_keys,  const TableView& right_keys,
    const TableView& left_payload, const TableView& right_payload)
{
    auto [left_idx, right_idx] = cudf::inner_join(*left_keys.inner, *right_keys.inner);
    return gather_combine(std::move(left_idx), std::move(right_idx),
                          *left_payload.inner, *right_payload.inner,
                          cudf::out_of_bounds_policy::DONT_CHECK,
                          cudf::out_of_bounds_policy::DONT_CHECK);
}

std::unique_ptr<Table> left_join_gather(
    const TableView& left_keys,  const TableView& right_keys,
    const TableView& left_payload, const TableView& right_payload)
{
    auto [left_idx, right_idx] = cudf::left_join(*left_keys.inner, *right_keys.inner);
    // Left map never contains sentinels (all left rows appear), right map uses INT32_MIN
    // for unmatched rows which NULLIFY converts to null output rows.
    return gather_combine(std::move(left_idx), std::move(right_idx),
                          *left_payload.inner, *right_payload.inner,
                          cudf::out_of_bounds_policy::DONT_CHECK,
                          cudf::out_of_bounds_policy::NULLIFY);
}

std::unique_ptr<Table> full_join_gather(
    const TableView& left_keys,  const TableView& right_keys,
    const TableView& left_payload, const TableView& right_payload)
{
    auto [left_idx, right_idx] = cudf::full_join(*left_keys.inner, *right_keys.inner);
    return gather_combine(std::move(left_idx), std::move(right_idx),
                          *left_payload.inner, *right_payload.inner,
                          cudf::out_of_bounds_policy::NULLIFY,
                          cudf::out_of_bounds_policy::NULLIFY);
}

HashJoin::HashJoin() = default;

HashJoin::~HashJoin() = default;

std::unique_ptr<HashJoin> hash_join_create(
    const TableView& build_keys, bool nulls_equal)
{
    auto result = std::make_unique<HashJoin>();
    auto compare_nulls = nulls_equal
        ? cudf::null_equality::EQUAL
        : cudf::null_equality::UNEQUAL;
    result->inner = std::make_unique<cudf::hash_join>(*build_keys.inner, compare_nulls);
    return result;
}

std::unique_ptr<Table> hash_join_inner_join_gather(
    const HashJoin& join,
    const TableView& probe_keys,
    const TableView& build_payload,
    const TableView& probe_payload)
{
    auto [probe_idx, build_idx] = join.inner->inner_join(*probe_keys.inner);
    return gather_combine(std::move(build_idx), std::move(probe_idx),
                          *build_payload.inner, *probe_payload.inner,
                          cudf::out_of_bounds_policy::DONT_CHECK,
                          cudf::out_of_bounds_policy::DONT_CHECK);
}

std::unique_ptr<Table> left_semi_join_gather(
    const TableView& left_keys, const TableView& right_keys,
    const TableView& left_payload)
{
    cudf::filtered_join fj(*right_keys.inner, cudf::null_equality::EQUAL,
                           cudf::set_as_build_table::RIGHT, cudf::get_default_stream());
    auto idx = fj.semi_join(*left_keys.inner);
    auto idx_col = uvector_to_column(std::move(idx));
    auto result = std::make_unique<Table>();
    result->inner = cudf::gather(*left_payload.inner, idx_col->view(),
                                 cudf::out_of_bounds_policy::DONT_CHECK);
    return result;
}

std::unique_ptr<Table> left_anti_join_gather(
    const TableView& left_keys, const TableView& right_keys,
    const TableView& left_payload)
{
    cudf::filtered_join fj(*right_keys.inner, cudf::null_equality::EQUAL,
                           cudf::set_as_build_table::RIGHT, cudf::get_default_stream());
    auto idx = fj.anti_join(*left_keys.inner);
    auto idx_col = uvector_to_column(std::move(idx));
    auto result = std::make_unique<Table>();
    result->inner = cudf::gather(*left_payload.inner, idx_col->view(),
                                 cudf::out_of_bounds_policy::DONT_CHECK);
    return result;
}

std::unique_ptr<Table> cross_join(const TableView& left, const TableView& right) {
    auto result = std::make_unique<Table>();
    result->inner = cudf::cross_join(*left.inner, *right.inner);
    return result;
}

} // namespace libcudf_bridge
