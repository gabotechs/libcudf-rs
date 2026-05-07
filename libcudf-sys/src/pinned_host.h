#pragma once

#include <cstddef>
#include <memory>

#include <cudf/utilities/pinned_memory.hpp>
#include <rmm/resource_ref.hpp>

namespace libcudf_bridge {
    /// Thin wrapper around `rmm::host_device_async_resource_ref`, the type
    /// returned by `cudf::get_pinned_memory_resource()`. When
    /// `cudf::config_default_pinned_memory_resource` has been called, the
    /// underlying RMM pool reuses already-allocated pinned slabs.
    ///
    /// `allocate_sync` / `deallocate_sync` ignore the stream parameter on the
    /// pinned MR (per RMM `pinned_host_memory_resource`), so the call is
    /// synchronous from the caller's perspective.
    struct HostDeviceAsyncResourceRef {
        // `mutable` because the underlying resource_ref's allocate_sync /
        // deallocate_sync are non-const, but conceptually the wrapper is just
        // a handle; the cuDF MR behind it serializes its own concurrent
        // allocate / deallocate. This lets the bridge expose `&Self` methods
        // instead of `Pin<&mut Self>`, so a single global instance can be
        // shared without locking on the Rust side.
        mutable rmm::host_device_async_resource_ref inner;

        explicit HostDeviceAsyncResourceRef(rmm::host_device_async_resource_ref ref);

        /// 1:1 with `host_device_async_resource_ref::allocate_sync`. Returned
        /// as `size_t` because cxx does not currently expose `*mut u8` across
        /// the bridge.
        [[nodiscard]] size_t allocate_sync(size_t bytes) const;

        /// 1:1 with `host_device_async_resource_ref::deallocate_sync`.
        void deallocate_sync(size_t ptr, size_t bytes) const;
    };

    /// 1:1 with `cudf::get_pinned_memory_resource()`.
    std::unique_ptr<HostDeviceAsyncResourceRef> get_pinned_memory_resource();

    /// 1:1 with `cudaStreamSynchronize(0)` (the default stream).
    void cuda_default_stream_synchronize();
} // namespace libcudf_bridge
