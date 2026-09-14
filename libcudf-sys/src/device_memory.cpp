#include "device_memory.h"

#include <rmm/cuda_device.hpp>
#include <rmm/mr/per_device_resource.hpp>

namespace libcudf_bridge {
    CudaMemoryResource::CudaMemoryResource()
        : inner(std::make_unique<rmm::mr::cuda_memory_resource>()) {}

    rmm::mr::cuda_memory_resource* CudaMemoryResource::get() const {
        return inner.get();
    }

    std::unique_ptr<CudaMemoryResource> make_cuda_memory_resource() {
        return std::make_unique<CudaMemoryResource>();
    }

    AvailableDeviceMemory::AvailableDeviceMemory(
        const std::size_t free_bytes,
        const std::size_t total_bytes) : free(free_bytes), total(total_bytes) {}

    std::size_t AvailableDeviceMemory::free_bytes() const {
        return free;
    }

    std::size_t AvailableDeviceMemory::total_bytes() const {
        return total;
    }

    PoolMemoryResource::PoolMemoryResource(const rmm::mr::cuda_memory_resource& upstream,
                                           const std::size_t initial_size)
        : inner(std::make_unique<rmm::mr::pool_memory_resource>(
              cuda::mr::any_resource<cuda::mr::device_accessible>(upstream),
              initial_size)) {}

    PoolMemoryResource::PoolMemoryResource(const rmm::mr::cuda_memory_resource& upstream,
                                           const std::size_t initial_size,
                                           const std::size_t maximum_size)
        : inner(std::make_unique<rmm::mr::pool_memory_resource>(
              cuda::mr::any_resource<cuda::mr::device_accessible>(upstream),
              initial_size,
              maximum_size)) {}

    rmm::mr::pool_memory_resource* PoolMemoryResource::get() const {
        return inner.get();
    }

    std::unique_ptr<PoolMemoryResource> make_pool_memory_resource(
        const CudaMemoryResource& upstream,
        const std::size_t initial_size) {
        return std::make_unique<PoolMemoryResource>(*upstream.get(), initial_size);
    }

    std::unique_ptr<PoolMemoryResource> make_pool_memory_resource_with_maximum(
        const CudaMemoryResource& upstream,
        const std::size_t initial_size,
        const std::size_t maximum_size) {
        return std::make_unique<PoolMemoryResource>(*upstream.get(), initial_size, maximum_size);
    }

    std::unique_ptr<DeviceAsyncResourceRef> make_device_async_resource_ref(
        const PoolMemoryResource& resource) {
        return std::make_unique<DeviceAsyncResourceRef>(
            rmm::device_async_resource_ref{*resource.get()});
    }

    std::unique_ptr<AvailableDeviceMemory> available_device_memory() {
        const auto [free, total] = rmm::available_device_memory();
        return std::make_unique<AvailableDeviceMemory>(free, total);
    }
} // namespace libcudf_bridge
