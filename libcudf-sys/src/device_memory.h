#pragma once

#include <cstddef>
#include <memory>

#include <rmm/mr/cuda_memory_resource.hpp>
#include <rmm/mr/pool_memory_resource.hpp>

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
    std::unique_ptr<CudaMemoryResource> make_cuda_memory_resource();

    /// Owning wrapper around `rmm::mr::pool_memory_resource` backed by
    /// `rmm::mr::cuda_memory_resource`. Sub-allocates from a single up-front
    /// `cudaMalloc` slab.
    struct PoolMemoryResource {
        std::unique_ptr<rmm::mr::pool_memory_resource<rmm::mr::cuda_memory_resource>> inner;

        PoolMemoryResource(rmm::mr::cuda_memory_resource* upstream,
                           std::size_t initial_size,
                           std::size_t max_size);

        [[nodiscard]] rmm::mr::pool_memory_resource<rmm::mr::cuda_memory_resource>* get() const;
    };

    /// Construct an RMM pool memory resource. Borrows the upstream resource for
    /// the pool's lifetime.
    std::unique_ptr<PoolMemoryResource> make_pool_memory_resource(
        const CudaMemoryResource& upstream,
        std::size_t initial_size,
        std::size_t max_size);

    /// Install the pool as RMM's current device resource.
    void set_current_device_resource(const PoolMemoryResource& resource);

    /// Return total VRAM bytes on the current CUDA device.
    std::size_t total_device_memory();
} // namespace libcudf_bridge
