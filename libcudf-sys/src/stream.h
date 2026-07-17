#pragma once

#include <cstdint>
#include <memory>
#include <cudf/utilities/default_stream.hpp>
#include <cuda/stream>
#include <cuda_runtime_api.h>
#include <rmm/cuda_stream.hpp>
#include <rmm/cuda_stream_view.hpp>
#include "rust/cxx.h"

namespace libcudf_bridge {
    struct CudaEvent;

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

    /// Non-owning wrapper for an upstream CCCL CUDA stream ref.
    struct CudaStreamRef {
        cuda::stream_ref inner;

        explicit CudaStreamRef(cuda::stream_ref stream);

        ~CudaStreamRef();

        /// Create and record a CUDA event on this stream.
        [[nodiscard]] std::unique_ptr<CudaEvent> record_event(uint32_t flags) const;

        /// Make this stream wait until the CUDA event has completed.
        void wait_event(const CudaEvent& event) const;
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

    /// Owning wrapper for a CUDA event.
    struct CudaEvent {
        cuda::event inner;

        /// Own an upstream CCCL CUDA event.
        explicit CudaEvent(cuda::event event);

        ~CudaEvent();

        /// Return whether this wrapper owns a live CUDA event.
        [[nodiscard]] bool is_valid() const;

        /// Record this CUDA event on the given stream.
        void record(const CudaStreamRef& stream) const;

        /// Block until this CUDA event has completed.
        void sync() const;

        /// Return true if this CUDA event has completed.
        [[nodiscard]] bool is_done() const;
    };

    /// Create a CUDA stream using the default sync-default creation flag.
    [[nodiscard]] std::unique_ptr<CudaStream> cuda_stream_create();

    /// Create a CUDA stream with an explicit raw flag value.
    [[nodiscard]] std::unique_ptr<CudaStream> cuda_stream_create_with_flags(uint32_t flags);

    /// Return a non-owning view for an owned CUDA stream.
    [[nodiscard]] std::unique_ptr<CudaStreamView> cuda_stream_view(const CudaStream& stream);

    /// Convert an RMM CUDA stream view into an upstream CCCL CUDA stream ref.
    [[nodiscard]] std::unique_ptr<CudaStreamRef> cuda_stream_ref_from_view(const CudaStreamView& stream);

    /// Create a CUDA event on a device with explicit upstream event flags.
    [[nodiscard]] std::unique_ptr<CudaEvent> cuda_event_create_on_device(int32_t device_id, uint32_t flags);

    /// Get cuDF's current default stream.
    [[nodiscard]] std::unique_ptr<CudaStreamView> get_default_stream();

    /// Check whether cuDF is using the CUDA per-thread default stream.
    [[nodiscard]] bool is_ptds_enabled();

    /// Return the current CUDA device ordinal.
    [[nodiscard]] int32_t cuda_get_device();

    /// Set the current CUDA device ordinal.
    void cuda_set_device(int32_t device_id);

    /// Direct wrapper for cudaMemcpy. Pointer values are integer-encoded for cxx.
    [[nodiscard]] int32_t cuda_memcpy(uintptr_t destination,
                                      uintptr_t source,
                                      size_t count,
                                      int32_t kind);

    /// Direct wrapper for cudaGetErrorString.
    [[nodiscard]] rust::String cuda_get_error_string(int32_t error);
} // namespace libcudf_bridge
