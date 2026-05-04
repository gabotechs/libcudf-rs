#pragma once

#include <cstdint>
#include <memory>
#include <rmm/cuda_stream.hpp>
#include <rmm/cuda_stream_view.hpp>

namespace libcudf_bridge {
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
    };

    /// Create a CUDA stream using the default sync-default creation flag.
    std::unique_ptr<CudaStream> cuda_stream_create();

    /// Create a CUDA stream with an explicit raw flag value.
    std::unique_ptr<CudaStream> cuda_stream_create_with_flags(uint32_t flags);
} // namespace libcudf_bridge
