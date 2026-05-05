#include "pinned_host.h"

#include <cstdio>
#include <cuda_runtime.h>
#include <sstream>
#include <stdexcept>

namespace libcudf_bridge {
    namespace {
        void throw_on_cuda_error(cudaError_t err, const char* what) {
            if (err == cudaSuccess) return;
            std::ostringstream msg;
            msg << what << ": " << cudaGetErrorString(err);
            throw std::runtime_error(msg.str());
        }
    } // namespace

    PinnedHostAlloc::PinnedHostAlloc(size_t bytes) : data_ptr(nullptr), size_bytes(bytes) {
        void* raw = nullptr;
        throw_on_cuda_error(cudaMallocHost(&raw, bytes), "cudaMallocHost");
        data_ptr = static_cast<uint8_t*>(raw);
    }

    PinnedHostAlloc::~PinnedHostAlloc() {
        if (data_ptr == nullptr) return;
        // Reaching the destructor with a non-null pointer means the caller
        // never invoked `pinned_host_free`. We can't error out from a
        // destructor, can't safely throw (it would call std::terminate during
        // unwinding), and don't want to silently call cudaFreeHost (which
        // would swallow any failure). So we log and leak. That's the best we
        // can do.
        std::fprintf(stderr,
                     "libcudf_bridge: PinnedHostAlloc destructed without pinned_host_free; "
                     "leaking %zu bytes at %p\n",
                     size_bytes, static_cast<void*>(data_ptr));
    }

    size_t PinnedHostAlloc::data() const {
        return reinterpret_cast<size_t>(data_ptr);
    }

    size_t PinnedHostAlloc::len() const {
        return size_bytes;
    }

    std::unique_ptr<PinnedHostAlloc> pinned_host_alloc(size_t bytes) {
        return std::make_unique<PinnedHostAlloc>(bytes);
    }

    void pinned_host_free(std::unique_ptr<PinnedHostAlloc> alloc) {
        if (alloc == nullptr || alloc->data_ptr == nullptr) return;
        cudaError_t const err = cudaFreeHost(alloc->data_ptr);
        // Clear the pointer regardless so the destructor's leak-detect path
        // doesn't fire for an allocation we *attempted* to free. If the free
        // itself failed we still surface that to the caller via throw.
        alloc->data_ptr = nullptr;
        throw_on_cuda_error(err, "cudaFreeHost");
    }

    void cuda_default_stream_synchronize() {
        throw_on_cuda_error(cudaStreamSynchronize(0), "cudaStreamSynchronize(default)");
    }
} // namespace libcudf_bridge
