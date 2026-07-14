#include "concatenate.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/concatenate.hpp>

#include <stdexcept>
#include <vector>

namespace libcudf_bridge {
    std::unique_ptr<Table> concatenate(
        rust::Slice<const std::unique_ptr<TableView>> views,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr) {
        std::vector<cudf::table_view> table_views;
        table_views.reserve(views.size());
        for (auto const& view: views) {
            if (!view || !view->inner) {
                throw std::invalid_argument("Cannot concatenate a null table view");
            }
            table_views.push_back(*view->inner);
        }

        auto result = std::make_unique<Table>();
        result->inner = cudf::concatenate(table_views, stream.inner, mr.inner);
        return result;
    }

    std::unique_ptr<Column> concatenate(
        rust::Slice<const std::unique_ptr<ColumnView>> views,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr) {
        std::vector<cudf::column_view> column_views;
        column_views.reserve(views.size());
        for (auto const& view: views) {
            if (!view || !view->inner) {
                throw std::invalid_argument("Cannot concatenate a null column view");
            }
            column_views.push_back(*view->inner);
        }

        auto result = std::make_unique<Column>();
        result->inner = cudf::concatenate(column_views, stream.inner, mr.inner);
        return result;
    }
} // namespace libcudf_bridge
