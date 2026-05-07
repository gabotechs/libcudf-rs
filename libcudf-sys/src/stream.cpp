#include "stream.h"

#include <stdexcept>

namespace libcudf_bridge {
    namespace {
        constexpr uint32_t kCudaStreamFlagSyncDefault = 0;
        constexpr uint32_t kCudaStreamFlagNonBlocking = 1;

        static_assert(
            static_cast<uint32_t>(rmm::cuda_stream::flags::sync_default) ==
            kCudaStreamFlagSyncDefault);
        static_assert(
            static_cast<uint32_t>(rmm::cuda_stream::flags::non_blocking) ==
            kCudaStreamFlagNonBlocking);

        [[nodiscard]] rmm::cuda_stream::flags to_rmm_flags(const uint32_t flags) {
            return static_cast<rmm::cuda_stream::flags>(flags);
        }
    } // namespace

    CudaStreamView::CudaStreamView(rmm::cuda_stream_view stream) : inner(stream) {}

    CudaStreamView::~CudaStreamView() = default;

    bool CudaStreamView::is_default() const {
        return inner.is_default();
    }

    bool CudaStreamView::is_per_thread_default() const {
        return inner.is_per_thread_default();
    }

    void CudaStreamView::synchronize() const {
        inner.synchronize();
    }

    /// By default, create a stream that synchronizes with the default stream
    /// (ie. this uses [`rmm::cuda_stream::flags::sync_default`]).
    CudaStream::CudaStream() : inner(std::make_unique<rmm::cuda_stream>()) {}

    CudaStream::CudaStream(const uint32_t flags)
        : inner(std::make_unique<rmm::cuda_stream>(to_rmm_flags(flags))) {}

    CudaStream::~CudaStream() = default;

    // A wrapper is valid as long as it still owns an underlying RMM stream.
    bool CudaStream::is_valid() const {
        return inner && inner->is_valid();
    }

    rmm::cuda_stream_view CudaStream::view() const {
        // In case `inner` gets moved by assigning one `CudaStream` to another.
        if (!inner) {
            throw std::runtime_error("Cannot get view of null CUDA stream");
        }
        return inner->view();
    }

    void CudaStream::synchronize() const {
        if (!inner) {
            throw std::runtime_error("Cannot synchronize null CUDA stream");
        }
        inner->synchronize();
    }

    std::unique_ptr<CudaStream> cuda_stream_create() {
        return std::make_unique<CudaStream>();
    }

    std::unique_ptr<CudaStream> cuda_stream_create_with_flags(const uint32_t flags) {
        return std::make_unique<CudaStream>(flags);
    }

    std::unique_ptr<CudaStreamView> cuda_stream_view(const CudaStream& stream) {
        return std::make_unique<CudaStreamView>(stream.view());
    }

    std::unique_ptr<CudaStreamView> get_default_stream() {
        return std::make_unique<CudaStreamView>(cudf::get_default_stream());
    }

    bool is_ptds_enabled() {
        return cudf::is_ptds_enabled();
    }
} // namespace libcudf_bridge
