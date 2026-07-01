#include "stream.h"

#include <rmm/detail/error.hpp>

#include <stdexcept>
#include <utility>

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
        static_assert(static_cast<uint32_t>(cuda::event_flags::none) == cudaEventDefault);
        static_assert(
            static_cast<uint32_t>(cuda::event_flags::blocking_sync) == cudaEventBlockingSync);
        static_assert(
            static_cast<uint32_t>(cuda::event_flags::interprocess) == cudaEventInterprocess);

        [[nodiscard]] rmm::cuda_stream::flags to_rmm_flags(const uint32_t flags) {
            return static_cast<rmm::cuda_stream::flags>(flags);
        }

        [[nodiscard]] cuda::event_flags to_event_flags(const uint32_t flags) {
            return static_cast<cuda::event_flags>(flags);
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

    CudaStreamRef::CudaStreamRef(cuda::stream_ref stream) : inner(stream) {}

    CudaStreamRef::~CudaStreamRef() = default;

    std::unique_ptr<CudaEvent> CudaStreamRef::record_event(const uint32_t flags) const {
        auto event = inner.record_event(to_event_flags(flags));
        return std::make_unique<CudaEvent>(std::move(event));
    }

    void CudaStreamRef::wait_event(const CudaEvent& event) const {
        if (!event.is_valid()) {
            throw std::runtime_error("Cannot wait on null CUDA event");
        }
        inner.wait(event.inner);
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

    CudaEvent::CudaEvent(cuda::event event) : inner(std::move(event)) {}

    CudaEvent::~CudaEvent() = default;

    bool CudaEvent::is_valid() const {
        return static_cast<bool>(inner);
    }

    void CudaEvent::record(const CudaStreamRef& stream) const {
        if (!is_valid()) {
            throw std::runtime_error("Cannot record null CUDA event");
        }
        inner.record(stream.inner);
    }

    void CudaEvent::sync() const {
        if (!is_valid()) {
            throw std::runtime_error("Cannot synchronize null CUDA event");
        }
        inner.sync();
    }

    bool CudaEvent::is_done() const {
        if (!is_valid()) {
            throw std::runtime_error("Cannot query null CUDA event");
        }
        return inner.is_done();
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

    std::unique_ptr<CudaStreamRef> cuda_stream_ref_from_view(const CudaStreamView& stream) {
        return std::make_unique<CudaStreamRef>(static_cast<cuda::stream_ref>(stream.inner));
    }

    std::unique_ptr<CudaEvent> cuda_event_create_on_device(
        const int32_t device_id,
        const uint32_t flags) {
        return std::make_unique<CudaEvent>(
            cuda::event(cuda::device_ref{device_id}, to_event_flags(flags)));
    }

    std::unique_ptr<CudaStreamView> get_default_stream() {
        return std::make_unique<CudaStreamView>(cudf::get_default_stream());
    }

    bool is_ptds_enabled() {
        return cudf::is_ptds_enabled();
    }

    int32_t cuda_get_device() {
        int device{};
        RMM_CUDA_TRY(cudaGetDevice(&device));
        return device;
    }

    void cuda_set_device(const int32_t device_id) {
        RMM_CUDA_TRY(cudaSetDevice(device_id));
    }
} // namespace libcudf_bridge
