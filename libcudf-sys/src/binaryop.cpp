#include "binaryop.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/binaryop.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/types.hpp>

namespace libcudf_bridge {
    static_assert(static_cast<int32_t>(cudf::binary_operator::ADD) == 0);
    static_assert(static_cast<int32_t>(cudf::binary_operator::SUB) == 1);
    static_assert(static_cast<int32_t>(cudf::binary_operator::MUL) == 2);
    static_assert(static_cast<int32_t>(cudf::binary_operator::DIV) == 3);
    static_assert(static_cast<int32_t>(cudf::binary_operator::TRUE_DIV) == 4);
    static_assert(static_cast<int32_t>(cudf::binary_operator::FLOOR_DIV) == 5);
    static_assert(static_cast<int32_t>(cudf::binary_operator::MOD) == 6);
    static_assert(static_cast<int32_t>(cudf::binary_operator::PMOD) == 7);
    static_assert(static_cast<int32_t>(cudf::binary_operator::PYMOD) == 8);
    static_assert(static_cast<int32_t>(cudf::binary_operator::POW) == 9);
    static_assert(static_cast<int32_t>(cudf::binary_operator::INT_POW) == 10);
    static_assert(static_cast<int32_t>(cudf::binary_operator::LOG_BASE) == 11);
    static_assert(static_cast<int32_t>(cudf::binary_operator::ATAN2) == 12);
    static_assert(static_cast<int32_t>(cudf::binary_operator::SHIFT_LEFT) == 13);
    static_assert(static_cast<int32_t>(cudf::binary_operator::SHIFT_RIGHT) == 14);
    static_assert(static_cast<int32_t>(cudf::binary_operator::SHIFT_RIGHT_UNSIGNED) == 15);
    static_assert(static_cast<int32_t>(cudf::binary_operator::BITWISE_AND) == 16);
    static_assert(static_cast<int32_t>(cudf::binary_operator::BITWISE_OR) == 17);
    static_assert(static_cast<int32_t>(cudf::binary_operator::BITWISE_XOR) == 18);
    static_assert(static_cast<int32_t>(cudf::binary_operator::LOGICAL_AND) == 19);
    static_assert(static_cast<int32_t>(cudf::binary_operator::LOGICAL_OR) == 20);
    static_assert(static_cast<int32_t>(cudf::binary_operator::EQUAL) == 21);
    static_assert(static_cast<int32_t>(cudf::binary_operator::NOT_EQUAL) == 22);
    static_assert(static_cast<int32_t>(cudf::binary_operator::LESS) == 23);
    static_assert(static_cast<int32_t>(cudf::binary_operator::GREATER) == 24);
    static_assert(static_cast<int32_t>(cudf::binary_operator::LESS_EQUAL) == 25);
    static_assert(static_cast<int32_t>(cudf::binary_operator::GREATER_EQUAL) == 26);
    static_assert(static_cast<int32_t>(cudf::binary_operator::NULL_EQUALS) == 27);
    static_assert(static_cast<int32_t>(cudf::binary_operator::NULL_NOT_EQUALS) == 28);
    static_assert(static_cast<int32_t>(cudf::binary_operator::NULL_MAX) == 29);
    static_assert(static_cast<int32_t>(cudf::binary_operator::NULL_MIN) == 30);
    static_assert(static_cast<int32_t>(cudf::binary_operator::GENERIC_BINARY) == 31);
    static_assert(static_cast<int32_t>(cudf::binary_operator::NULL_LOGICAL_AND) == 32);
    static_assert(static_cast<int32_t>(cudf::binary_operator::NULL_LOGICAL_OR) == 33);
    static_assert(static_cast<int32_t>(cudf::binary_operator::INVALID_BINARY) == 34);

    // Binary operation: column op column
    std::unique_ptr<Column> binary_operation_col_col(
        const ColumnView &lhs,
        const ColumnView &rhs,
        int32_t op,
        const DataType &output_type) {
        const auto binary_op = static_cast<cudf::binary_operator>(op);

        auto result_col = cudf::binary_operation(
            *lhs.inner,
            *rhs.inner,
            binary_op,
            output_type.inner
        );

        return std::make_unique<Column>(column_from_unique_ptr(std::move(result_col)));
    }

    std::unique_ptr<Column> binary_operation_col_col_on(
        const ColumnView &lhs,
        const ColumnView &rhs,
        int32_t op,
        const DataType &output_type,
        const CudaStream &stream) {
        const auto binary_op = static_cast<cudf::binary_operator>(op);

        auto result_col = cudf::binary_operation(
            *lhs.inner,
            *rhs.inner,
            binary_op,
            output_type.inner,
            stream.view()
        );

        return std::make_unique<Column>(column_from_unique_ptr(std::move(result_col)));
    }

    // Binary operation: column op scalar
    std::unique_ptr<Column> binary_operation_col_scalar(
        const ColumnView &lhs,
        const Scalar &rhs,
        int32_t op,
        const DataType &output_type) {
        const auto binary_op = static_cast<cudf::binary_operator>(op);

        auto result_col = cudf::binary_operation(
            *lhs.inner,
            *rhs.inner,
            binary_op,
            output_type.inner
        );

        return std::make_unique<Column>(column_from_unique_ptr(std::move(result_col)));
    }

    std::unique_ptr<Column> binary_operation_col_scalar_on(
        const ColumnView &lhs,
        const Scalar &rhs,
        int32_t op,
        const DataType &output_type,
        const CudaStream &stream) {
        const auto binary_op = static_cast<cudf::binary_operator>(op);

        auto result_col = cudf::binary_operation(
            *lhs.inner,
            *rhs.inner,
            binary_op,
            output_type.inner,
            stream.view()
        );

        return std::make_unique<Column>(column_from_unique_ptr(std::move(result_col)));
    }

    // Binary operation: scalar op column
    std::unique_ptr<Column> binary_operation_scalar_col(
        const Scalar &lhs,
        const ColumnView &rhs,
        int32_t op,
        const DataType &output_type) {
        const auto binary_op = static_cast<cudf::binary_operator>(op);

        auto result_col = cudf::binary_operation(
            *lhs.inner,
            *rhs.inner,
            binary_op,
            output_type.inner
        );

        return std::make_unique<Column>(column_from_unique_ptr(std::move(result_col)));
    }

    std::unique_ptr<Column> binary_operation_scalar_col_on(
        const Scalar &lhs,
        const ColumnView &rhs,
        int32_t op,
        const DataType &output_type,
        const CudaStream &stream) {
        const auto binary_op = static_cast<cudf::binary_operator>(op);

        auto result_col = cudf::binary_operation(
            *lhs.inner,
            *rhs.inner,
            binary_op,
            output_type.inner,
            stream.view()
        );

        return std::make_unique<Column>(column_from_unique_ptr(std::move(result_col)));
    }
} // namespace libcudf_bridge
