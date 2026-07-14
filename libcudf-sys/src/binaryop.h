#pragma once

#include <memory>
#include <cstdint>
#include "rust/cxx.h"
#include "column.h"
#include "scalar.h"
#include "data_type.h"
#include "stream.h"
#include "memory_resource.h"

namespace libcudf_bridge {

    [[nodiscard]] std::unique_ptr<Column> binary_operation(
        const ColumnView &lhs,
        const ColumnView &rhs,
        int32_t op,
        const DataType &output_type,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr);

    [[nodiscard]] std::unique_ptr<Column> binary_operation(
        const ColumnView &lhs,
        const Scalar &rhs,
        int32_t op,
        const DataType &output_type,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr);

    [[nodiscard]] std::unique_ptr<Column> binary_operation(
        const Scalar &lhs,
        const ColumnView &rhs,
        int32_t op,
        const DataType &output_type,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr);

    [[nodiscard]] std::unique_ptr<Column> binary_operation(
        const ColumnView &lhs,
        const ColumnView &rhs,
        rust::Str ptx,
        const DataType &output_type,
        const CudaStreamView &stream,
        const DeviceAsyncResourceRef &mr);
} // namespace libcudf_bridge
