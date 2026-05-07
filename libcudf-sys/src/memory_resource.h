#pragma once

#include <memory>

#include <cudf/utilities/memory_resource.hpp>
#include <rmm/resource_ref.hpp>

namespace libcudf_bridge {
    /// Non-owning wrapper for an RMM device async resource reference.
    struct DeviceAsyncResourceRef {
        rmm::device_async_resource_ref inner;

        explicit DeviceAsyncResourceRef(rmm::device_async_resource_ref resource);

        ~DeviceAsyncResourceRef();
    };

    /// Return cuDF's current device memory resource reference.
    std::unique_ptr<DeviceAsyncResourceRef> get_current_device_resource_ref();

    /// Set cuDF's current device memory resource reference.
    std::unique_ptr<DeviceAsyncResourceRef> set_current_device_resource_ref(
        const DeviceAsyncResourceRef& resource);

    /// Reset cuDF's current device memory resource reference to the initial resource.
    std::unique_ptr<DeviceAsyncResourceRef> reset_current_device_resource_ref();

    /// Compare two device async resource references.
    bool device_async_resource_ref_equal(
        const DeviceAsyncResourceRef& lhs,
        const DeviceAsyncResourceRef& rhs);
} // namespace libcudf_bridge
