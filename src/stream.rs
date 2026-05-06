use cxx::UniquePtr;

/// Stream creation flags for CUDA stream-backed cuDF execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CuDFStreamFlags {
    /// Create a stream that synchronizes with the default stream.
    SyncDefault,
    /// Create a non-blocking stream that does not synchronize with the default stream.
    NonBlocking,
}

const CUDA_STREAM_FLAG_SYNC_DEFAULT: u32 = 0;
const CUDA_STREAM_FLAG_NON_BLOCKING: u32 = 1;

impl From<CuDFStreamFlags> for u32 {
    fn from(value: CuDFStreamFlags) -> Self {
        match value {
            CuDFStreamFlags::SyncDefault => CUDA_STREAM_FLAG_SYNC_DEFAULT,
            CuDFStreamFlags::NonBlocking => CUDA_STREAM_FLAG_NON_BLOCKING,
        }
    }
}

/// Owning Rust wrapper for a CUDA stream used by cuDF operations.
///
/// This type owns an opaque C++ `rmm::cuda_stream`. Dropping `CuDFStream`
/// destroys that underlying stream.
///
/// The handle may be shared across host threads. Work enqueued onto the same
/// stream executes in order; sharing the handle does not by itself synchronize
/// access to GPU memory used by those operations.
pub struct CuDFStream {
    // Kept alive so the underlying C++ `rmm::cuda_stream` is destroyed on
    // drop. Accessed via `inner()` from within the crate.
    #[allow(dead_code)]
    inner: UniquePtr<libcudf_sys::ffi::CudaStream>,
}

impl CuDFStream {
    /// Create a stream using the sync-default creation flag.
    pub fn new() -> Self {
        Self {
            inner: libcudf_sys::ffi::cuda_stream_create(),
        }
    }

    /// Create a stream with explicit creation flags.
    pub fn with_flags(flags: CuDFStreamFlags) -> Self {
        Self {
            inner: libcudf_sys::ffi::cuda_stream_create_with_flags(flags.into()),
        }
    }

    /// Get a reference to the underlying FFI stream handle.
    #[allow(dead_code)]
    pub(crate) fn inner(&self) -> &libcudf_sys::ffi::CudaStream {
        self.inner.as_ref().expect("CudaStream should not be null")
    }
}

impl Default for CuDFStream {
    fn default() -> Self {
        Self::new()
    }
}
