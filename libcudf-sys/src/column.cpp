#include "column.h"
#include "data_type.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/column/column.hpp>
#include <cudf/scalar/scalar.hpp>
#include <cudf/interop.hpp>

#include <nanoarrow/nanoarrow.h>
#include <nanoarrow/nanoarrow_device.h>


namespace libcudf_bridge {
    // ColumnView implementation
    ColumnView::ColumnView() : inner(nullptr) {
    }

    ColumnView::~ColumnView() = default;

    size_t ColumnView::size() const {
        return inner->size();
    }

    void ColumnView::to_arrow_array(uint8_t *out_array_ptr) const {
        auto device_array_unique = cudf::to_arrow_host(*this->inner);
        auto *out_array = reinterpret_cast<ArrowDeviceArray *>(out_array_ptr);
        *out_array = *device_array_unique.get();
        device_array_unique.release();
    }

    [[nodiscard]] uint64_t ColumnView::data_ptr() const {
        if (!inner) {
            return 0;
        }
        return reinterpret_cast<uint64_t>(inner->head());
    }

    [[nodiscard]] std::unique_ptr<DataType> ColumnView::data_type() const {
        auto dtype = inner->type();
        auto type_id = static_cast<int32_t>(dtype.id());

        // Only pass scale for decimal types
        if (dtype.id() == cudf::type_id::DECIMAL32 ||
            dtype.id() == cudf::type_id::DECIMAL64 ||
            dtype.id() == cudf::type_id::DECIMAL128) {
            return std::make_unique<DataType>(type_id, dtype.scale());
        } else {
            return std::make_unique<DataType>(type_id);
        }
    }

    [[nodiscard]] std::unique_ptr<ColumnView> ColumnView::clone() const {
        auto cloned = std::make_unique<ColumnView>();
        if (inner) {
            cloned->inner = std::make_unique<cudf::column_view>(*inner);
        }
        return cloned;
    }

    // Column implementation
    Column::Column() : inner(nullptr) {
    }

    Column::~Column() = default;

    size_t Column::size() const {
        if (!inner) {
            return 0;
        }
        return inner->size();
    }

    [[nodiscard]] std::unique_ptr<ColumnView> Column::view() const {
        auto result = std::make_unique<ColumnView>();
        result->inner = std::make_unique<cudf::column_view>(inner->view());
        return result;
    }

    [[nodiscard]] std::unique_ptr<DataType> Column::data_type() const {
        auto dtype = inner->type();
        auto type_id = static_cast<int32_t>(dtype.id());

        // Only pass scale for decimal types
        if (dtype.id() == cudf::type_id::DECIMAL32 ||
            dtype.id() == cudf::type_id::DECIMAL64 ||
            dtype.id() == cudf::type_id::DECIMAL128) {
            return std::make_unique<DataType>(type_id, dtype.scale());
        } else {
            return std::make_unique<DataType>(type_id);
        }
    }

    // Helper function to create Column from unique_ptr
    Column column_from_unique_ptr(std::unique_ptr<cudf::column> col) {
        Column c;
        c.inner = std::move(col);
        return c;
    }

    // Scalar implementation
    Scalar::Scalar() : inner(nullptr) {
    }

    Scalar::~Scalar() = default;

    bool Scalar::is_valid() const {
        if (!inner) {
            return false;
        }
        return inner->is_valid();
    }
} // namespace libcudf_bridge
