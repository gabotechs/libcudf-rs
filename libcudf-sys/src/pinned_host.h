#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

namespace libcudf_bridge {
    /// Owning wrapper for a pinned (page-locked) host allocation
    /// (ie. `cudaMallocHost`).
    ///
    /// Pinned host memory can be DMA'd directly by the GPU without an internal
    /// staging copy, so `cudaMemcpyAsync` from a pinned source is fully
    /// asynchronous.
    ///
    /// See https://docs.nvidia.com/cuda/cuda-runtime-api/api-sync-behavior.html.
    ///
    /// The owning `cudaMemcpyAsync` must complete before this wrapper is
    /// destroyed (otherwise we may free the allocation before or while
    /// it is in use); callers should synchronize the relevant stream
    /// (see [`cuda_default_stream_synchronize`]) before dropping it.
    ///
    /// # Lifecycle
    ///
    /// The destructor does **not** free the underlying allocation — callers
    /// must invoke [`pinned_host_free`] on the owning `unique_ptr` to release
    /// the memory and surface any `cudaFreeHost` error. If the destructor runs
    /// while still holding a non-null `data_ptr`, that indicates a missed
    /// release on the caller's side; the destructor logs and leaks rather
    /// than silently calling `cudaFreeHost` (which would either swallow an
    /// error or, if it threw, crash the process via `std::terminate`).
    struct PinnedHostAlloc {
        uint8_t* data_ptr;
        size_t size_bytes;

        explicit PinnedHostAlloc(size_t bytes);
        ~PinnedHostAlloc();

        // Non-copyable, non-movable: the wrapper owns a single CUDA allocation.
        PinnedHostAlloc(PinnedHostAlloc const&)            = delete;
        PinnedHostAlloc& operator=(PinnedHostAlloc const&) = delete;
        PinnedHostAlloc(PinnedHostAlloc&&)                 = delete;
        PinnedHostAlloc& operator=(PinnedHostAlloc&&)      = delete;

        /// Raw pointer to the allocation, returned as an integer because cxx
        /// does not currently expose `*mut u8` return values across the bridge.
        /// Valid until [`pinned_host_free`] is invoked.
        [[nodiscard]] size_t data() const;

        /// Allocation size in bytes.
        [[nodiscard]] size_t len() const;
    };

    /// Allocate `bytes` of pinned host memory via `cudaMallocHost`.
    /// Throws on allocation failure.
    std::unique_ptr<PinnedHostAlloc> pinned_host_alloc(size_t bytes);

    /// Release a pinned host allocation.
    ///
    /// Consumes the owning `unique_ptr`, calls `cudaFreeHost`, and throws on
    /// any error so that the failure surfaces back to the Rust caller as a
    /// `Result::Err` rather than being silently swallowed inside a destructor.
    void pinned_host_free(std::unique_ptr<PinnedHostAlloc> alloc);

    /// Block the calling thread until all work submitted to the CUDA default
    /// stream has completed.
    ///
    /// Example uses:
    /// - In the H -> D copy path, we may call this to ensure the copy is finished
    ///   before freeing a pinned memory allocation.
    void cuda_default_stream_synchronize();
} // namespace libcudf_bridge
