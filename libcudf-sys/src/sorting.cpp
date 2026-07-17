#include "sorting.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/sorting.hpp>
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/column/column.hpp>
#include <cudf/types.hpp>

#include <stdexcept>

namespace libcudf_bridge {
    static_assert(static_cast<int32_t>(cudf::order::ASCENDING) == 0);
    static_assert(static_cast<int32_t>(cudf::order::DESCENDING) == 1);
    static_assert(static_cast<int32_t>(cudf::null_order::AFTER) == 0);
    static_assert(static_cast<int32_t>(cudf::null_order::BEFORE) == 1);

    namespace {
        const cudf::table_view& require_table_view(const TableView& input) {
            if (!input.inner) {
                throw std::invalid_argument("Cannot sort a null table view");
            }
            return *input.inner;
        }

        std::vector<cudf::order> to_orders(const rust::Slice<const int32_t> values) {
            std::vector<cudf::order> result;
            result.reserve(values.size());
            for (auto value: values) {
                result.push_back(static_cast<cudf::order>(value));
            }
            return result;
        }

        std::vector<cudf::null_order> to_null_orders(const rust::Slice<const int32_t> values) {
            std::vector<cudf::null_order> result;
            result.reserve(values.size());
            for (auto value: values) {
                result.push_back(static_cast<cudf::null_order>(value));
            }
            return result;
        }
    } // namespace

    std::unique_ptr<Table> sort(
        const TableView &input,
        const rust::Slice<const int32_t> column_order,
        const rust::Slice<const int32_t> null_precedence,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr) {
        auto result_table = cudf::sort(
            require_table_view(input),
            to_orders(column_order),
            to_null_orders(null_precedence),
            stream.inner,
            mr.inner);

        auto wrapped = std::make_unique<Table>();
        wrapped->inner = std::move(result_table);
        return wrapped;
    }

    std::unique_ptr<Table> stable_sort(
        const TableView &input,
        const rust::Slice<const int32_t> column_order,
        const rust::Slice<const int32_t> null_precedence,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr) {
        auto result_table = cudf::stable_sort(
            require_table_view(input),
            to_orders(column_order),
            to_null_orders(null_precedence),
            stream.inner,
            mr.inner);

        auto wrapped = std::make_unique<Table>();
        wrapped->inner = std::move(result_table);
        return wrapped;
    }

    std::unique_ptr<Column> sorted_order(
        const TableView &input,
        const rust::Slice<const int32_t> column_order,
        const rust::Slice<const int32_t> null_precedence,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr) {
        auto result_col = cudf::sorted_order(
            require_table_view(input),
            to_orders(column_order),
            to_null_orders(null_precedence),
            stream.inner,
            mr.inner);

        return std::make_unique<Column>(column_from_unique_ptr(std::move(result_col)));
    }

    std::unique_ptr<Column> stable_sorted_order(
        const TableView &input,
        const rust::Slice<const int32_t> column_order,
        const rust::Slice<const int32_t> null_precedence,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr) {
        auto result_col = cudf::stable_sorted_order(
            require_table_view(input),
            to_orders(column_order),
            to_null_orders(null_precedence),
            stream.inner,
            mr.inner);

        return std::make_unique<Column>(column_from_unique_ptr(std::move(result_col)));
    }

    bool is_sorted(
        const TableView &input,
        rust::Slice<const int32_t> column_order,
        rust::Slice<const int32_t> null_precedence,
        const CudaStreamView &stream) {
        return cudf::is_sorted(
            require_table_view(input),
            to_orders(column_order),
            to_null_orders(null_precedence),
            stream.inner);
    }

    std::unique_ptr<Table> sort_by_key(
        const TableView &values,
        const TableView &keys,
        const rust::Slice<const int32_t> column_order,
        const rust::Slice<const int32_t> null_precedence,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr) {
        auto result_table = cudf::sort_by_key(
            require_table_view(values),
            require_table_view(keys),
            to_orders(column_order),
            to_null_orders(null_precedence),
            stream.inner,
            mr.inner);

        auto wrapped = std::make_unique<Table>();
        wrapped->inner = std::move(result_table);
        return wrapped;
    }

    std::unique_ptr<Table> stable_sort_by_key(
        const TableView &values,
        const TableView &keys,
        const rust::Slice<const int32_t> column_order,
        const rust::Slice<const int32_t> null_precedence,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr) {
        auto result_table = cudf::stable_sort_by_key(
            require_table_view(values),
            require_table_view(keys),
            to_orders(column_order),
            to_null_orders(null_precedence),
            stream.inner,
            mr.inner);

        auto wrapped = std::make_unique<Table>();
        wrapped->inner = std::move(result_table);
        return wrapped;
    }
} // namespace libcudf_bridge
