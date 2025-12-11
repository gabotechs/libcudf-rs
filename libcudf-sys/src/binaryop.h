#pragma once

#include <memory>
#include <cstdint>
#include "rust/cxx.h"
#include "column.h"
#include "scalar.h"
#include "data_type.h"

namespace libcudf_bridge {

    // Binary operations - direct cuDF mappings
    std::unique_ptr<Column> binary_operation_col_col(
        const ColumnView &lhs,
        const ColumnView &rhs,
        int32_t op,
        const DataType &output_type);

    std::unique_ptr<Column> binary_operation_col_scalar(
        const ColumnView &lhs,
        const Scalar &rhs,
        int32_t op,
        const DataType &output_type);

    std::unique_ptr<Column> binary_operation_scalar_col(
        const Scalar &lhs,
        const ColumnView &rhs,
        int32_t op,
        const DataType &output_type);
} // namespace libcudf_bridge
