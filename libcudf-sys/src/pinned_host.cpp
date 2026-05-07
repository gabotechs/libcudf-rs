#include "pinned_host.h"

#include <cuda_runtime.h>
#include <utility>

namespace libcudf_bridge {
    size_t cuda_malloc_host(const size_t bytes) {
        void* raw = nullptr;
        cudaMallocHost(&raw, bytes);
        return reinterpret_cast<size_t>(raw);
    }

    void cuda_free_host(const size_t ptr) {
        if (ptr == 0) return;
        cudaFreeHost(reinterpret_cast<void*>(ptr));
    }

    HostDeviceAsyncResourceRef::HostDeviceAsyncResourceRef(rmm::host_device_async_resource_ref ref)
        : inner(std::move(ref)) {}

    size_t HostDeviceAsyncResourceRef::allocate_sync(const size_t bytes) const {
        return reinterpret_cast<size_t>(inner.allocate_sync(bytes));
    }

    void HostDeviceAsyncResourceRef::deallocate_sync(const size_t ptr, const size_t bytes) const {
        inner.deallocate_sync(reinterpret_cast<void *>(ptr), bytes);
    }

    std::unique_ptr<HostDeviceAsyncResourceRef> get_pinned_memory_resource() {
        return std::make_unique<HostDeviceAsyncResourceRef>(cudf::get_pinned_memory_resource());
    }

    void cuda_default_stream_synchronize() {
        cudaStreamSynchronize(nullptr);
    }
} // namespace libcudf_bridge
