#include "pinned_host.h"

#include <cuda_runtime.h>

namespace libcudf_bridge {
    HostDeviceAsyncResourceRef::HostDeviceAsyncResourceRef(rmm::host_device_async_resource_ref ref)
        : inner(ref) {}

    size_t HostDeviceAsyncResourceRef::allocate_sync(const size_t bytes) {
        return reinterpret_cast<size_t>(inner.allocate_sync(bytes));
    }

    void HostDeviceAsyncResourceRef::deallocate_sync(const size_t ptr, const size_t bytes) {
        inner.deallocate_sync(reinterpret_cast<void *>(ptr), bytes);
    }

    std::unique_ptr<HostDeviceAsyncResourceRef> get_pinned_memory_resource() {
        return std::make_unique<HostDeviceAsyncResourceRef>(cudf::get_pinned_memory_resource());
    }

    void cuda_default_stream_synchronize() {
        cudaStreamSynchronize(nullptr);
    }
} // namespace libcudf_bridge
