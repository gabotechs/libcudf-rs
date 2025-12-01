#include "bridge.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/binaryop.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/types.hpp>

namespace libcudf_bridge {
    // Binary operation: column op column
    std::unique_ptr<Column> binary_operation_col_col(
        const ColumnView &lhs,
        const ColumnView &rhs,
        int32_t op,
        int32_t output_type_id) {
        const auto binary_op = static_cast<cudf::binary_operator>(op);
        const auto output_type = cudf::data_type{static_cast<cudf::type_id>(output_type_id)};

        auto result_col = cudf::binary_operation(
            *lhs.inner,
            *rhs.inner,
            binary_op,
            output_type
        );

        return std::make_unique<Column>(column_from_unique_ptr(std::move(result_col)));
    }

    // Binary operation: column op scalar
    std::unique_ptr<Column> binary_operation_col_scalar(
        const ColumnView &lhs,
        const Scalar &rhs,
        int32_t op,
        int32_t output_type_id) {
        const auto binary_op = static_cast<cudf::binary_operator>(op);
        const auto output_type = cudf::data_type{static_cast<cudf::type_id>(output_type_id)};

        auto result_col = cudf::binary_operation(
            *lhs.inner,
            *rhs.inner,
            binary_op,
            output_type
        );

        return std::make_unique<Column>(column_from_unique_ptr(std::move(result_col)));
    }

    // Binary operation: scalar op column
    std::unique_ptr<Column> binary_operation_scalar_col(
        const Scalar &lhs,
        const ColumnView &rhs,
        int32_t op,
        int32_t output_type_id) {
        const auto binary_op = static_cast<cudf::binary_operator>(op);
        const auto output_type = cudf::data_type{static_cast<cudf::type_id>(output_type_id)};

        auto result_col = cudf::binary_operation(
            *lhs.inner,
            *rhs.inner,
            binary_op,
            output_type
        );

        return std::make_unique<Column>(column_from_unique_ptr(std::move(result_col)));
    }
} // namespace libcudf_bridge
