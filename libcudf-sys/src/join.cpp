#include "join.h"
#include <cudf/join/join.hpp>
#include <cudf/join/filtered_join.hpp>
#include <cudf/column/column.hpp>
#include <cudf/utilities/default_stream.hpp>

namespace libcudf_bridge {

static std::unique_ptr<cudf::column> uvector_to_column(
    std::unique_ptr<rmm::device_uvector<cudf::size_type>> vec)
{
    return std::make_unique<cudf::column>(std::move(*vec), rmm::device_buffer{}, 0);
}

static std::unique_ptr<Column> uvector_to_bridge_column(
    std::unique_ptr<rmm::device_uvector<cudf::size_type>> vec)
{
    auto result = std::make_unique<Column>();
    result->inner = uvector_to_column(std::move(vec));
    return result;
}

static std::unique_ptr<JoinIndices> make_join_indices(
    std::unique_ptr<rmm::device_uvector<cudf::size_type>> left_idx,
    std::unique_ptr<rmm::device_uvector<cudf::size_type>> right_idx)
{
    auto result = std::make_unique<JoinIndices>();
    result->left = uvector_to_bridge_column(std::move(left_idx));
    result->right = uvector_to_bridge_column(std::move(right_idx));
    return result;
}

JoinIndices::JoinIndices() = default;

JoinIndices::~JoinIndices() = default;

std::unique_ptr<Column> JoinIndices::release_left() {
    return std::move(left);
}

std::unique_ptr<Column> JoinIndices::release_right() {
    return std::move(right);
}

HashJoinIndices::HashJoinIndices() = default;

HashJoinIndices::~HashJoinIndices() = default;

std::unique_ptr<Column> HashJoinIndices::release_probe() {
    return std::move(probe);
}

std::unique_ptr<Column> HashJoinIndices::release_build() {
    return std::move(build);
}

static std::unique_ptr<HashJoinIndices> make_hash_join_indices(
    std::unique_ptr<rmm::device_uvector<cudf::size_type>> probe_idx,
    std::unique_ptr<rmm::device_uvector<cudf::size_type>> build_idx)
{
    auto result = std::make_unique<HashJoinIndices>();
    result->probe = uvector_to_bridge_column(std::move(probe_idx));
    result->build = uvector_to_bridge_column(std::move(build_idx));
    return result;
}

std::unique_ptr<JoinIndices> inner_join_indices(
    const TableView& left_keys,
    const TableView& right_keys)
{
    auto [left_idx, right_idx] = cudf::inner_join(*left_keys.inner, *right_keys.inner);
    return make_join_indices(std::move(left_idx), std::move(right_idx));
}

std::unique_ptr<JoinIndices> left_join_indices(
    const TableView& left_keys,
    const TableView& right_keys)
{
    auto [left_idx, right_idx] = cudf::left_join(*left_keys.inner, *right_keys.inner);
    return make_join_indices(std::move(left_idx), std::move(right_idx));
}

std::unique_ptr<JoinIndices> full_join_indices(
    const TableView& left_keys,
    const TableView& right_keys)
{
    auto [left_idx, right_idx] = cudf::full_join(*left_keys.inner, *right_keys.inner);
    return make_join_indices(std::move(left_idx), std::move(right_idx));
}

HashJoin::HashJoin() = default;

HashJoin::~HashJoin() = default;

std::unique_ptr<HashJoin> hash_join_create(
    const TableView& build_keys, int32_t null_equality)
{
    auto result = std::make_unique<HashJoin>();
    auto compare_nulls = static_cast<cudf::null_equality>(null_equality);
    result->inner = std::make_unique<cudf::hash_join>(*build_keys.inner, compare_nulls);
    return result;
}

std::unique_ptr<HashJoinIndices> hash_join_inner_join_indices(
    const HashJoin& join,
    const TableView& probe_keys)
{
    auto [probe_idx, build_idx] = join.inner->inner_join(*probe_keys.inner);
    return make_hash_join_indices(std::move(probe_idx), std::move(build_idx));
}

std::unique_ptr<HashJoinIndices> hash_join_left_join_indices(
    const HashJoin& join,
    const TableView& probe_keys)
{
    auto [probe_idx, build_idx] = join.inner->left_join(*probe_keys.inner);
    return make_hash_join_indices(std::move(probe_idx), std::move(build_idx));
}

std::unique_ptr<Column> left_semi_join_indices(
    const TableView& left_keys,
    const TableView& right_keys)
{
    cudf::filtered_join fj(*right_keys.inner, cudf::null_equality::EQUAL,
                           cudf::set_as_build_table::RIGHT, cudf::get_default_stream());
    return uvector_to_bridge_column(fj.semi_join(*left_keys.inner));
}

std::unique_ptr<Column> left_anti_join_indices(
    const TableView& left_keys,
    const TableView& right_keys)
{
    cudf::filtered_join fj(*right_keys.inner, cudf::null_equality::EQUAL,
                           cudf::set_as_build_table::RIGHT, cudf::get_default_stream());
    return uvector_to_bridge_column(fj.anti_join(*left_keys.inner));
}

std::unique_ptr<Table> cross_join(const TableView& left, const TableView& right) {
    auto result = std::make_unique<Table>();
    result->inner = cudf::cross_join(*left.inner, *right.inner);
    return result;
}

} // namespace libcudf_bridge
