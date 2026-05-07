#include "memory_resource.h"

namespace libcudf_bridge {
    DeviceAsyncResourceRef::DeviceAsyncResourceRef(rmm::device_async_resource_ref resource)
        : inner(resource) {}

    DeviceAsyncResourceRef::~DeviceAsyncResourceRef() = default;

    std::unique_ptr<DeviceAsyncResourceRef> get_current_device_resource_ref() {
        return std::make_unique<DeviceAsyncResourceRef>(cudf::get_current_device_resource_ref());
    }

    std::unique_ptr<DeviceAsyncResourceRef> set_current_device_resource_ref(
        const DeviceAsyncResourceRef& resource) {
        return std::make_unique<DeviceAsyncResourceRef>(
            cudf::set_current_device_resource_ref(resource.inner));
    }

    std::unique_ptr<DeviceAsyncResourceRef> reset_current_device_resource_ref() {
        return std::make_unique<DeviceAsyncResourceRef>(cudf::reset_current_device_resource_ref());
    }

    bool device_async_resource_ref_equal(
        const DeviceAsyncResourceRef& lhs,
        const DeviceAsyncResourceRef& rhs) {
        return lhs.inner == rhs.inner;
    }
} // namespace libcudf_bridge
