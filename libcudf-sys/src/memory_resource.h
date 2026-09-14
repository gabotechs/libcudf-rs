#pragma once

#include <memory>

#include <cudf/utilities/memory_resource.hpp>
#include <rmm/resource_ref.hpp>

#include <cuda/memory_resource>

namespace libcudf_bridge {
    /// Non-owning wrapper for an RMM device async resource reference.
    struct DeviceAsyncResourceRef {
        rmm::device_async_resource_ref inner;

        explicit DeviceAsyncResourceRef(rmm::device_async_resource_ref resource);

        ~DeviceAsyncResourceRef();
    };

    /// Owning wrapper for an RMM `any_resource`, the type cuDF returns when
    /// swapping the current device resource. It keeps the previous resource
    /// alive for as long as this handle lives.
    struct DeviceAnyResource {
        // Mutable because `rmm::device_async_resource_ref` only binds to a
        // non-const resource, and `as_ref` is logically a const observer.
        mutable cuda::mr::any_resource<cuda::mr::device_accessible> inner;

        explicit DeviceAnyResource(cuda::mr::any_resource<cuda::mr::device_accessible> resource);

        ~DeviceAnyResource();

        [[nodiscard]] std::unique_ptr<DeviceAsyncResourceRef> as_ref() const;
    };

    /// Return cuDF's current device memory resource reference.
    [[nodiscard]] std::unique_ptr<DeviceAsyncResourceRef> get_current_device_resource_ref();

    /// Set cuDF's current device memory resource reference, returning the previous one.
    [[nodiscard]] std::unique_ptr<DeviceAnyResource> set_current_device_resource_ref(
        const DeviceAsyncResourceRef& resource);

    /// Reset cuDF's current device memory resource to the initial resource,
    /// returning the previous one.
    [[nodiscard]] std::unique_ptr<DeviceAnyResource> reset_current_device_resource_ref();

    /// Compare two device async resource references.
    [[nodiscard]] bool device_async_resource_ref_equal(
        const DeviceAsyncResourceRef& lhs,
        const DeviceAsyncResourceRef& rhs);
} // namespace libcudf_bridge
