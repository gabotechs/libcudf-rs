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

    PoolMemoryResource::PoolMemoryResource(rmm::mr::cuda_memory_resource* upstream,
                                           const std::size_t initial_size,
                                           const std::size_t max_size)
        : inner(std::make_unique<rmm::mr::pool_memory_resource<rmm::mr::cuda_memory_resource>>(
              upstream, initial_size, max_size)) {}

    rmm::mr::pool_memory_resource<rmm::mr::cuda_memory_resource>* PoolMemoryResource::get() const {
        return inner.get();
    }

    std::unique_ptr<PoolMemoryResource> make_pool_memory_resource(
        const CudaMemoryResource& upstream,
        const std::size_t initial_size,
        const std::size_t max_size) {
        return std::make_unique<PoolMemoryResource>(upstream.get(), initial_size, max_size);
    }

    void set_current_device_resource(const PoolMemoryResource& resource) {
        rmm::mr::set_current_device_resource(resource.get());
    }

    std::size_t total_device_memory() {
        return rmm::available_device_memory().second;
    }
} // namespace libcudf_bridge
