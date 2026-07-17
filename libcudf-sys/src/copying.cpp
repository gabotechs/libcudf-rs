#include "copying.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/copying.hpp>

#include <functional>
#include <stdexcept>

namespace libcudf_bridge {
    static_assert(static_cast<int32_t>(cudf::out_of_bounds_policy::NULLIFY) == 0);
    static_assert(static_cast<int32_t>(cudf::out_of_bounds_policy::DONT_CHECK) == 1);

    std::unique_ptr<Table> gather(
        const TableView& source_table,
        const ColumnView& gather_map,
        int32_t out_of_bounds_policy,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr) {
        if (!source_table.inner || !gather_map.inner) {
            throw std::invalid_argument("Cannot gather from a null view");
        }
        auto result = std::make_unique<Table>();
        result->inner = cudf::gather(
            *source_table.inner,
            *gather_map.inner,
            static_cast<cudf::out_of_bounds_policy>(out_of_bounds_policy),
            stream.inner,
            mr.inner);
        return result;
    }

    std::unique_ptr<Table> scatter(
        const TableView& source,
        const ColumnView& indices,
        const TableView& target,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr) {
        if (!source.inner || !indices.inner || !target.inner) {
            throw std::invalid_argument("Cannot scatter a null view");
        }
        auto result = std::make_unique<Table>();
        result->inner = cudf::scatter(
            *source.inner, *indices.inner, *target.inner, stream.inner, mr.inner);
        return result;
    }

    std::unique_ptr<Table> scatter(
        rust::Slice<const Scalar *const> source,
        const ColumnView& indices,
        const TableView& target,
        const CudaStreamView& stream,
        const DeviceAsyncResourceRef& mr) {
        if (!indices.inner || !target.inner) {
            throw std::invalid_argument("Cannot scatter into a null view");
        }
        std::vector<std::reference_wrapper<cudf::scalar const>> scalars;
        scalars.reserve(source.size());
        for (auto *scalar: source) {
            if (scalar == nullptr || !scalar->inner) {
                throw std::invalid_argument("Cannot scatter a null scalar");
            }
            scalars.emplace_back(*scalar->inner);
        }

        auto result = std::make_unique<Table>();
        result->inner = cudf::scatter(
            scalars, *indices.inner, *target.inner, stream.inner, mr.inner);
        return result;
    }

    ColumnViews::ColumnViews(std::vector<cudf::column_view> views) : inner(std::move(views)) {}
    ColumnViews::~ColumnViews() = default;
    size_t ColumnViews::len() const { return inner.size(); }
    bool ColumnViews::is_empty() const { return inner.empty(); }

    std::unique_ptr<ColumnView> ColumnViews::get(size_t index) const {
        auto result = std::make_unique<ColumnView>();
        result->inner = std::make_unique<cudf::column_view>(inner.at(index));
        return result;
    }

    TableViews::TableViews(std::vector<cudf::table_view> views) : inner(std::move(views)) {}
    TableViews::~TableViews() = default;
    size_t TableViews::len() const { return inner.size(); }
    bool TableViews::is_empty() const { return inner.empty(); }

    std::unique_ptr<TableView> TableViews::get(size_t index) const {
        auto result = std::make_unique<TableView>();
        result->inner = std::make_unique<cudf::table_view>(inner.at(index));
        return result;
    }

    std::unique_ptr<ColumnViews> slice(
        const ColumnView& column,
        rust::Slice<const int32_t> indices,
        const CudaStreamView& stream) {
        if (!column.inner) {
            throw std::invalid_argument("Cannot slice a null column view");
        }
        auto views = cudf::slice(
            *column.inner,
            cudf::host_span<cudf::size_type const>{indices.data(), indices.size()},
            stream.inner);
        return std::make_unique<ColumnViews>(std::move(views));
    }

    std::unique_ptr<TableViews> slice(
        const TableView& table,
        rust::Slice<const int32_t> indices,
        const CudaStreamView& stream) {
        if (!table.inner) {
            throw std::invalid_argument("Cannot slice a null table view");
        }
        auto views = cudf::slice(
            *table.inner,
            cudf::host_span<cudf::size_type const>{indices.data(), indices.size()},
            stream.inner);
        return std::make_unique<TableViews>(std::move(views));
    }
} // namespace libcudf_bridge
