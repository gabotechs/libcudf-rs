#include "stream.h"

#include <stdexcept>

namespace libcudf_bridge {
    namespace {
        constexpr uint32_t kCudaStreamFlagSyncDefault = 0;
        constexpr uint32_t kCudaStreamFlagNonBlocking = 1;

        // Map our flags to [`rmm::cuda_stream::flags`].
        [[nodiscard]] rmm::cuda_stream::flags to_rmm_flags(const uint32_t flags) {
            switch (flags) {
                case kCudaStreamFlagSyncDefault:
                    return rmm::cuda_stream::flags::sync_default;
                case kCudaStreamFlagNonBlocking:
                    return rmm::cuda_stream::flags::non_blocking;
            }

            throw std::invalid_argument("Unsupported CUDA stream flags");
        }
    } // namespace

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

    std::unique_ptr<CudaStream> cuda_stream_create() {
        return std::make_unique<CudaStream>();
    }

    std::unique_ptr<CudaStream> cuda_stream_create_with_flags(const uint32_t flags) {
        return std::make_unique<CudaStream>(flags);
    }
} // namespace libcudf_bridge
