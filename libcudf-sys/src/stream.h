#pragma once

#include <cstdint>
#include <memory>
#include <cudf/utilities/default_stream.hpp>
#include <rmm/cuda_stream.hpp>
#include <rmm/cuda_stream_view.hpp>

namespace libcudf_bridge {
    /// Non-owning wrapper for an RMM CUDA stream view.
    struct CudaStreamView {
        rmm::cuda_stream_view inner;

        explicit CudaStreamView(rmm::cuda_stream_view stream);

        ~CudaStreamView();

        /// Return true if this is the CUDA legacy default stream.
        [[nodiscard]] bool is_default() const;

        /// Return true if this is the CUDA per-thread default stream.
        [[nodiscard]] bool is_per_thread_default() const;

        /// Synchronize the viewed CUDA stream.
        void synchronize() const;
    };

    /// Owning wrapper for an RMM CUDA stream.
    struct CudaStream {
        std::unique_ptr<rmm::cuda_stream> inner;

        /// Construct a stream using RMM's sync-default stream creation flag.
        CudaStream();

        /// Construct a stream with an explicit raw flag value.
        explicit CudaStream(uint32_t flags);

        ~CudaStream();

        /// Return whether this wrapper still owns a live CUDA stream.
        ///
        /// This becomes `false` if the wrapper has been moved-from and no longer
        /// owns its underlying `rmm::cuda_stream`.
        [[nodiscard]] bool is_valid() const;

        /// Return a non-owning stream view for passing into cuDF APIs.
        [[nodiscard]] rmm::cuda_stream_view view() const;

        /// Synchronize the owned CUDA stream.
        void synchronize() const;
    };

    /// Create a CUDA stream using the default sync-default creation flag.
    std::unique_ptr<CudaStream> cuda_stream_create();

    /// Create a CUDA stream with an explicit raw flag value.
    std::unique_ptr<CudaStream> cuda_stream_create_with_flags(uint32_t flags);

    /// Return a non-owning view for an owned CUDA stream.
    std::unique_ptr<CudaStreamView> cuda_stream_view(const CudaStream& stream);

    /// Get cuDF's current default stream.
    std::unique_ptr<CudaStreamView> get_default_stream();

    /// Check whether cuDF is using the CUDA per-thread default stream.
    bool is_ptds_enabled();
} // namespace libcudf_bridge
