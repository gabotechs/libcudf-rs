#include "bridge.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/column/column.hpp>
#include <cudf/scalar/scalar.hpp>

namespace libcudf_bridge {
    // ColumnView implementation
    ColumnView::ColumnView() : inner(nullptr) {
    }

    ColumnView::~ColumnView() = default;

    size_t ColumnView::size() const {
        return inner->size();
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
