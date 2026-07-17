#include "stream_compaction.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/stream_compaction.hpp>

#include <stdexcept>
#include <vector>

namespace libcudf_bridge {
    static_assert(static_cast<int32_t>(cudf::duplicate_keep_option::KEEP_ANY) == 0);
    static_assert(static_cast<int32_t>(cudf::duplicate_keep_option::KEEP_FIRST) == 1);
    static_assert(static_cast<int32_t>(cudf::duplicate_keep_option::KEEP_LAST) == 2);
    static_assert(static_cast<int32_t>(cudf::duplicate_keep_option::KEEP_NONE) == 3);
    static_assert(static_cast<int32_t>(cudf::null_equality::EQUAL) == 0);
    static_assert(static_cast<int32_t>(cudf::null_equality::UNEQUAL) == 1);
    static_assert(static_cast<int32_t>(cudf::nan_equality::ALL_EQUAL) == 0);
    static_assert(static_cast<int32_t>(cudf::nan_equality::UNEQUAL) == 1);

    std::unique_ptr<Table> apply_boolean_mask(
        const TableView& table,
        const ColumnView& boolean_mask,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr) {
        if (!table.inner || !boolean_mask.inner) {
            throw std::invalid_argument("Cannot apply a mask to a null view");
        }
        auto result = std::make_unique<Table>();
        result->inner = cudf::apply_boolean_mask(
            *table.inner, *boolean_mask.inner, stream.inner, mr.inner);
        return result;
    }

    std::unique_ptr<Table> distinct(
        const TableView& input,
        rust::Slice<const int32_t> keys,
        int32_t keep,
        int32_t nulls_equal,
        int32_t nans_equal,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr) {
        if (!input.inner) {
            throw std::invalid_argument("Cannot deduplicate a null table view");
        }
        std::vector<cudf::size_type> key_indices(keys.begin(), keys.end());
        auto result = std::make_unique<Table>();
        result->inner = cudf::distinct(
            *input.inner,
            key_indices,
            static_cast<cudf::duplicate_keep_option>(keep),
            static_cast<cudf::null_equality>(nulls_equal),
            static_cast<cudf::nan_equality>(nans_equal),
            stream.inner,
            mr.inner);
        return result;
    }
} // namespace libcudf_bridge
