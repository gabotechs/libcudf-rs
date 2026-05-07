#pragma once

#include <cstddef>
#include <memory>

#include <rmm/mr/cuda_memory_resource.hpp>
#include <rmm/mr/pool_memory_resource.hpp>

namespace libcudf_bridge {
    /// 1:1 owning wrapper around `rmm::mr::cuda_memory_resource`. Allocates GPU
    /// memory directly via `cudaMalloc` / `cudaFree`. Typically used as the
    /// upstream for [`PoolMemoryResource`] rather than directly.
    struct CudaMemoryResource {
        std::unique_ptr<rmm::mr::cuda_memory_resource> inner;

        CudaMemoryResource();

        [[nodiscard]] rmm::mr::cuda_memory_resource* get() const;
    };

    /// 1:1 with `rmm::mr::cuda_memory_resource()` constructor.
    std::unique_ptr<CudaMemoryResource> make_cuda_memory_resource();

    /// 1:1 owning wrapper around
    /// `rmm::mr::pool_memory_resource<rmm::mr::cuda_memory_resource>`.
    /// Sub-allocates from a single up-front `cudaMalloc` slab.
    struct PoolMemoryResource {
        std::unique_ptr<rmm::mr::pool_memory_resource<rmm::mr::cuda_memory_resource>> inner;

        PoolMemoryResource(rmm::mr::cuda_memory_resource* upstream,
                           std::size_t initial_size,
                           std::size_t max_size);

        [[nodiscard]] rmm::mr::pool_memory_resource<rmm::mr::cuda_memory_resource>* get() const;
    };

    /// 1:1 with the `rmm::mr::pool_memory_resource(upstream, initial, max)`
    /// constructor. Borrows the upstream MR for the pool's lifetime.
    std::unique_ptr<PoolMemoryResource> make_pool_memory_resource(
        const CudaMemoryResource& upstream,
        std::size_t initial_size,
        std::size_t max_size);

    /// 1:1 with `rmm::mr::set_current_device_resource`.
    void set_current_device_resource(const PoolMemoryResource& resource);

    /// 1:1 with `rmm::available_device_memory()`. Returns total bytes of VRAM
    /// on the current CUDA device. (RMM also returns the free amount; we only
    /// expose total, which is what callers need for capacity-based sizing.)
    std::size_t total_device_memory();
} // namespace libcudf_bridge
