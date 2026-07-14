#pragma once

#include <cstddef>
#include <memory>

#include <rmm/mr/cuda_memory_resource.hpp>
#include <rmm/mr/pool_memory_resource.hpp>
#include "memory_resource.h"

namespace libcudf_bridge {
    /// Owning wrapper around `rmm::mr::cuda_memory_resource`. Allocates GPU memory
    /// directly via `cudaMalloc` / `cudaFree`. Typically used as the upstream for
    /// [`PoolMemoryResource`] rather than directly.
    struct CudaMemoryResource {
        std::unique_ptr<rmm::mr::cuda_memory_resource> inner;

        CudaMemoryResource();

        [[nodiscard]] rmm::mr::cuda_memory_resource* get() const;
    };

    /// Construct an RMM CUDA memory resource.
    [[nodiscard]] std::unique_ptr<CudaMemoryResource> make_cuda_memory_resource();

    /// Mechanical wrapper around the pair returned by rmm::available_device_memory.
    struct AvailableDeviceMemory {
        std::size_t free;
        std::size_t total;

        AvailableDeviceMemory(std::size_t free_bytes, std::size_t total_bytes);

        [[nodiscard]] std::size_t free_bytes() const;
        [[nodiscard]] std::size_t total_bytes() const;
    };

    /// Owning wrapper around `rmm::mr::pool_memory_resource` backed by
    /// `rmm::mr::cuda_memory_resource`. Sub-allocates from a single up-front
    /// `cudaMalloc` slab.
    struct PoolMemoryResource {
        std::unique_ptr<rmm::mr::pool_memory_resource<rmm::mr::cuda_memory_resource>> inner;

        PoolMemoryResource(rmm::mr::cuda_memory_resource* upstream,
                           std::size_t initial_size);

        PoolMemoryResource(rmm::mr::cuda_memory_resource* upstream,
                           std::size_t initial_size,
                           std::size_t maximum_size);

        [[nodiscard]] rmm::mr::pool_memory_resource<rmm::mr::cuda_memory_resource>* get() const;

    };

    /// Construct an RMM pool memory resource. Borrows the upstream resource for
    /// the pool's lifetime.
    [[nodiscard]] std::unique_ptr<PoolMemoryResource> make_pool_memory_resource(
        const CudaMemoryResource& upstream,
        std::size_t initial_size);

    /// Construct an RMM pool memory resource with an explicit maximum size.
    [[nodiscard]] std::unique_ptr<PoolMemoryResource> make_pool_memory_resource_with_maximum(
        const CudaMemoryResource& upstream,
        std::size_t initial_size,
        std::size_t maximum_size);

    [[nodiscard]] std::unique_ptr<DeviceAsyncResourceRef> make_device_async_resource_ref(
        const PoolMemoryResource& resource);

    /// Direct wrapper for rmm::available_device_memory.
    [[nodiscard]] std::unique_ptr<AvailableDeviceMemory> available_device_memory();
} // namespace libcudf_bridge
