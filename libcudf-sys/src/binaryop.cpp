#include "binaryop.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/binaryop.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/types.hpp>

#include <stdexcept>
#include <string>

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

    namespace {
        const cudf::column_view& require_column_view(const ColumnView& column) {
            if (!column.inner) {
                throw std::invalid_argument("Cannot apply a binary operation to a null column view");
            }
            return *column.inner;
        }

        const cudf::scalar& require_scalar(const Scalar& scalar) {
            if (!scalar.inner) {
                throw std::invalid_argument("Cannot apply a binary operation to a null scalar");
            }
            return *scalar.inner;
        }

        std::unique_ptr<Column> wrap(std::unique_ptr<cudf::column> column) {
            return std::make_unique<Column>(column_from_unique_ptr(std::move(column)));
        }
    } // namespace

    std::unique_ptr<Column> binary_operation(
        const ColumnView &lhs,
        const ColumnView &rhs,
        int32_t op,
        const DataType &output_type,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr) {
        const auto binary_op = static_cast<cudf::binary_operator>(op);

        return wrap(cudf::binary_operation(
            require_column_view(lhs),
            require_column_view(rhs),
            binary_op,
            output_type.inner,
            stream.inner,
            mr.inner));
    }

    std::unique_ptr<Column> binary_operation(
        const ColumnView &lhs,
        const Scalar &rhs,
        int32_t op,
        const DataType &output_type,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr) {
        const auto binary_op = static_cast<cudf::binary_operator>(op);

        return wrap(cudf::binary_operation(
            require_column_view(lhs),
            require_scalar(rhs),
            binary_op,
            output_type.inner,
            stream.inner,
            mr.inner));
    }

    std::unique_ptr<Column> binary_operation(
        const Scalar &lhs,
        const ColumnView &rhs,
        int32_t op,
        const DataType &output_type,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr) {
        const auto binary_op = static_cast<cudf::binary_operator>(op);

        return wrap(cudf::binary_operation(
            require_scalar(lhs),
            require_column_view(rhs),
            binary_op,
            output_type.inner,
            stream.inner,
            mr.inner));
    }

    std::unique_ptr<Column> binary_operation(
        const ColumnView &lhs,
        const ColumnView &rhs,
        const rust::Str ptx,
        const DataType &output_type,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr) {
        return wrap(cudf::binary_operation(
            require_column_view(lhs),
            require_column_view(rhs),
            std::string(ptx.data(), ptx.size()),
            output_type.inner,
            stream.inner,
            mr.inner));
    }
} // namespace libcudf_bridge
