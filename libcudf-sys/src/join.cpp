#include "join.h"

#include <cudf/column/column.hpp>
#include <cudf/join/filtered_join.hpp>
#include <cudf/join/join.hpp>
#include <cudf/utilities/span.hpp>

#include <limits>
#include <stdexcept>
#include <type_traits>

namespace libcudf_bridge {
namespace {
static_assert(std::is_same_v<cudf::size_type, int32_t>);
static_assert(static_cast<int32_t>(cudf::JoinNoMatch) == std::numeric_limits<int32_t>::min());
static_assert(static_cast<int32_t>(cudf::join_kind::INNER_JOIN) == 0);
static_assert(static_cast<int32_t>(cudf::join_kind::LEFT_JOIN) == 1);
static_assert(static_cast<int32_t>(cudf::join_kind::FULL_JOIN) == 2);
static_assert(static_cast<int32_t>(cudf::join_kind::LEFT_SEMI_JOIN) == 3);
static_assert(static_cast<int32_t>(cudf::join_kind::LEFT_ANTI_JOIN) == 4);
static_assert(static_cast<int32_t>(cudf::null_equality::EQUAL) == 0);
static_assert(static_cast<int32_t>(cudf::null_equality::UNEQUAL) == 1);
static_assert(static_cast<int32_t>(cudf::set_as_build_table::LEFT) == 0);
static_assert(static_cast<int32_t>(cudf::set_as_build_table::RIGHT) == 1);

std::unique_ptr<DeviceIndexVector> make_device_index_vector(
    std::unique_ptr<rmm::device_uvector<cudf::size_type>> vec)
{
    auto result = std::make_unique<DeviceIndexVector>();
    result->inner = std::move(vec);
    return result;
}

std::unique_ptr<JoinIndices> make_join_indices(
    std::unique_ptr<rmm::device_uvector<cudf::size_type>> left_idx,
    std::unique_ptr<rmm::device_uvector<cudf::size_type>> right_idx)
{
    auto result = std::make_unique<JoinIndices>();
    result->left = make_device_index_vector(std::move(left_idx));
    result->right = make_device_index_vector(std::move(right_idx));
    return result;
}

std::unique_ptr<HashJoinIndices> make_hash_join_indices(
    std::unique_ptr<rmm::device_uvector<cudf::size_type>> probe_idx,
    std::unique_ptr<rmm::device_uvector<cudf::size_type>> build_idx)
{
    auto result = std::make_unique<HashJoinIndices>();
    result->probe = make_device_index_vector(std::move(probe_idx));
    result->build = make_device_index_vector(std::move(build_idx));
    return result;
}

cudf::device_span<cudf::size_type const> index_span(const DeviceIndexVector& indices)
{
    if (!indices.inner) {
        throw std::runtime_error("Cannot use null device vector as join indices");
    }
    return cudf::device_span<cudf::size_type const>(indices.inner->data(), indices.inner->size());
}

cudf::hash_join const& require_hash_join(const HashJoin& join)
{
    if (!join.inner) {
        throw std::runtime_error("Cannot use null hash join");
    }
    return *join.inner;
}

cudf::filtered_join const& require_filtered_join(const FilteredJoin& join)
{
    if (!join.inner) {
        throw std::runtime_error("Cannot use null filtered join");
    }
    return *join.inner;
}
} // namespace

DeviceIndexVector::DeviceIndexVector() = default;

DeviceIndexVector::~DeviceIndexVector() = default;

size_t DeviceIndexVector::size() const {
    if (!inner) {
        throw std::runtime_error("Cannot get size of null device index vector");
    }
    return inner->size();
}

std::unique_ptr<ColumnView> DeviceIndexVector::view() const {
    if (!inner) {
        throw std::runtime_error("Cannot view null device index vector");
    }

    auto result = std::make_unique<ColumnView>();
    result->inner = std::make_unique<cudf::column_view>(
        cudf::data_type{cudf::type_id::INT32},
        static_cast<cudf::size_type>(inner->size()),
        inner->data(),
        nullptr,
        0);
    return result;
}

JoinIndices::JoinIndices() = default;

JoinIndices::~JoinIndices() = default;

std::unique_ptr<DeviceIndexVector> JoinIndices::release_left() {
    return std::move(left);
}

std::unique_ptr<DeviceIndexVector> JoinIndices::release_right() {
    return std::move(right);
}

HashJoinIndices::HashJoinIndices() = default;

HashJoinIndices::~HashJoinIndices() = default;

std::unique_ptr<DeviceIndexVector> HashJoinIndices::release_probe() {
    return std::move(probe);
}

std::unique_ptr<DeviceIndexVector> HashJoinIndices::release_build() {
    return std::move(build);
}

HashJoin::HashJoin() = default;

HashJoin::~HashJoin() = default;

FilteredJoin::FilteredJoin() = default;

FilteredJoin::~FilteredJoin() = default;

int32_t join_no_match() {
    return static_cast<int32_t>(cudf::JoinNoMatch);
}

std::unique_ptr<JoinIndices> inner_join_indices(
    const TableView& left_keys,
    const TableView& right_keys,
    int32_t null_equality,
    const CudaStreamView& stream,
    const DeviceAsyncResourceRef& mr)
{
    auto [left_idx, right_idx] = cudf::inner_join(
        *left_keys.inner,
        *right_keys.inner,
        static_cast<cudf::null_equality>(null_equality),
        stream.inner,
        mr.inner);
    return make_join_indices(std::move(left_idx), std::move(right_idx));
}

std::unique_ptr<JoinIndices> left_join_indices(
    const TableView& left_keys,
    const TableView& right_keys,
    int32_t null_equality,
    const CudaStreamView& stream,
    const DeviceAsyncResourceRef& mr)
{
    auto [left_idx, right_idx] = cudf::left_join(
        *left_keys.inner,
        *right_keys.inner,
        static_cast<cudf::null_equality>(null_equality),
        stream.inner,
        mr.inner);
    return make_join_indices(std::move(left_idx), std::move(right_idx));
}

std::unique_ptr<JoinIndices> full_join_indices(
    const TableView& left_keys,
    const TableView& right_keys,
    int32_t null_equality,
    const CudaStreamView& stream,
    const DeviceAsyncResourceRef& mr)
{
    auto [left_idx, right_idx] = cudf::full_join(
        *left_keys.inner,
        *right_keys.inner,
        static_cast<cudf::null_equality>(null_equality),
        stream.inner,
        mr.inner);
    return make_join_indices(std::move(left_idx), std::move(right_idx));
}

std::unique_ptr<JoinIndices> filter_join_indices(
    const TableView& left,
    const TableView& right,
    const DeviceIndexVector& left_indices,
    const DeviceIndexVector& right_indices,
    const AstExpressionTree& predicate,
    int32_t join_kind,
    const CudaStreamView& stream,
    const DeviceAsyncResourceRef& mr)
{
    if (predicate.inner.size() == 0) {
        throw std::runtime_error("Cannot filter join indices with an empty AST predicate");
    }

    auto [filtered_left, filtered_right] = cudf::filter_join_indices(
        *left.inner,
        *right.inner,
        index_span(left_indices),
        index_span(right_indices),
        predicate.inner.back(),
        static_cast<cudf::join_kind>(join_kind),
        stream.inner,
        mr.inner);
    return make_join_indices(std::move(filtered_left), std::move(filtered_right));
}

std::unique_ptr<HashJoin> hash_join_create(
    const TableView& build_keys,
    int32_t null_equality,
    const CudaStreamView& stream)
{
    auto result = std::make_unique<HashJoin>();
    result->inner = std::make_unique<cudf::hash_join>(
        *build_keys.inner,
        static_cast<cudf::null_equality>(null_equality),
        stream.inner);
    return result;
}

std::unique_ptr<HashJoinIndices> hash_join_inner_join_indices(
    const HashJoin& join,
    const TableView& probe_keys,
    const CudaStreamView& stream,
    const DeviceAsyncResourceRef& mr)
{
    auto [probe_idx, build_idx] = require_hash_join(join).inner_join(
        *probe_keys.inner,
        {},
        stream.inner,
        mr.inner);
    return make_hash_join_indices(std::move(probe_idx), std::move(build_idx));
}

std::unique_ptr<HashJoinIndices> hash_join_left_join_indices(
    const HashJoin& join,
    const TableView& probe_keys,
    const CudaStreamView& stream,
    const DeviceAsyncResourceRef& mr)
{
    auto [probe_idx, build_idx] = require_hash_join(join).left_join(
        *probe_keys.inner,
        {},
        stream.inner,
        mr.inner);
    return make_hash_join_indices(std::move(probe_idx), std::move(build_idx));
}

std::unique_ptr<FilteredJoin> filtered_join_create(
    const TableView& build_keys,
    int32_t null_equality,
    int32_t set_as_build_table,
    const CudaStreamView& stream)
{
    auto result = std::make_unique<FilteredJoin>();
    result->inner = std::make_unique<cudf::filtered_join>(
        *build_keys.inner,
        static_cast<cudf::null_equality>(null_equality),
        static_cast<cudf::set_as_build_table>(set_as_build_table),
        stream.inner);
    return result;
}

std::unique_ptr<DeviceIndexVector> filtered_join_semi_join(
    const FilteredJoin& join,
    const TableView& probe_keys,
    const CudaStreamView& stream,
    const DeviceAsyncResourceRef& mr)
{
    return make_device_index_vector(
        require_filtered_join(join).semi_join(*probe_keys.inner, stream.inner, mr.inner));
}

std::unique_ptr<DeviceIndexVector> filtered_join_anti_join(
    const FilteredJoin& join,
    const TableView& probe_keys,
    const CudaStreamView& stream,
    const DeviceAsyncResourceRef& mr)
{
    return make_device_index_vector(
        require_filtered_join(join).anti_join(*probe_keys.inner, stream.inner, mr.inner));
}

std::unique_ptr<Table> cross_join(
    const TableView& left,
    const TableView& right,
    const CudaStreamView& stream,
    const DeviceAsyncResourceRef& mr)
{
    auto result = std::make_unique<Table>();
    result->inner = cudf::cross_join(*left.inner, *right.inner, stream.inner, mr.inner);
    return result;
}

} // namespace libcudf_bridge
