#include "bridge.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/sorting.hpp>
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/column/column.hpp>
#include <cudf/types.hpp>

namespace libcudf_bridge {
    // Sort a table
    std::unique_ptr<Table> sort_table(
        const TableView &input,
        const rust::Slice<const int32_t> column_order,
        const rust::Slice<const int32_t> null_precedence) {
        std::vector<cudf::order> orders;
        for (auto ord: column_order) {
            orders.push_back(static_cast<cudf::order>(ord));
        }

        std::vector<cudf::null_order> null_orders;
        for (auto null_ord: null_precedence) {
            null_orders.push_back(static_cast<cudf::null_order>(null_ord));
        }

        auto result_table = cudf::sort(*input.inner, orders, null_orders);

        auto wrapped = std::make_unique<Table>();
        wrapped->inner = std::move(result_table);
        return wrapped;
    }

    // Stable sort a table
    std::unique_ptr<Table> stable_sort_table(
        const TableView &input,
        const rust::Slice<const int32_t> column_order,
        const rust::Slice<const int32_t> null_precedence) {
        std::vector<cudf::order> orders;
        for (auto ord: column_order) {
            orders.push_back(static_cast<cudf::order>(ord));
        }

        std::vector<cudf::null_order> null_orders;
        for (auto null_ord: null_precedence) {
            null_orders.push_back(static_cast<cudf::null_order>(null_ord));
        }

        auto result_table = cudf::stable_sort(*input.inner, orders, null_orders);

        auto wrapped = std::make_unique<Table>();
        wrapped->inner = std::move(result_table);
        return wrapped;
    }

    // Get sorted order indices
    std::unique_ptr<Column> sorted_order(
        const TableView &input,
        const rust::Slice<const int32_t> column_order,
        const rust::Slice<const int32_t> null_precedence) {
        std::vector<cudf::order> orders;
        for (auto ord: column_order) {
            orders.push_back(static_cast<cudf::order>(ord));
        }

        std::vector<cudf::null_order> null_orders;
        for (auto null_ord: null_precedence) {
            null_orders.push_back(static_cast<cudf::null_order>(null_ord));
        }

        auto result_col = cudf::sorted_order(*input.inner, orders, null_orders);

        return std::make_unique<Column>(column_from_unique_ptr(std::move(result_col)));
    }

    // Check if table is sorted
    bool is_sorted(
        const TableView &input,
        rust::Slice<const int32_t> column_order,
        rust::Slice<const int32_t> null_precedence) {
        std::vector<cudf::order> orders;
        for (auto ord: column_order) {
            orders.push_back(static_cast<cudf::order>(ord));
        }

        std::vector<cudf::null_order> null_orders;
        for (auto null_ord: null_precedence) {
            null_orders.push_back(static_cast<cudf::null_order>(null_ord));
        }

        return cudf::is_sorted(*input.inner, orders, null_orders);
    }
} // namespace libcudf_bridge
