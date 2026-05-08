use cxx::UniquePtr;

use crate::{CuDFError, Result};
use libcudf_sys::ffi;

/// Stream creation flags for CUDA stream-backed cuDF execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum CuDFStreamFlags {
    /// Create a stream that synchronizes with the default stream.
    SyncDefault = 0,
    /// Create a non-blocking stream that does not synchronize with the default stream.
    NonBlocking = 1,
}

impl From<CuDFStreamFlags> for u32 {
    fn from(value: CuDFStreamFlags) -> Self {
        value as u32
    }
}

/// Owning Rust wrapper for a CUDA stream used by cuDF operations.
///
/// For the default stream, use [`ffi::get_default_stream`] to get the default stream
/// view instead.
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
    inner: UniquePtr<libcudf_sys::ffi::CudaStream>,
}

impl CuDFStream {
    /// Try to create a stream using the sync-default creation flag.
    pub fn try_new() -> Result<Self> {
        Ok(Self {
            inner: libcudf_sys::ffi::cuda_stream_create()?,
        })
    }

    /// Create a stream using the sync-default creation flag.
    pub fn new() -> Self {
        Self::try_new().expect("failed to create CUDA stream")
    }

    /// Try to create a stream with explicit creation flags.
    pub fn try_with_flags(flags: CuDFStreamFlags) -> Result<Self> {
        Ok(Self {
            inner: libcudf_sys::ffi::cuda_stream_create_with_flags(flags.into())?,
        })
    }

    /// Create a stream with explicit creation flags.
    pub fn with_flags(flags: CuDFStreamFlags) -> Self {
        Self::try_with_flags(flags).expect("failed to create CUDA stream")
    }

    /// Block until all work submitted to this stream has completed.
    pub fn synchronize(&self) -> Result<()> {
        self.inner().synchronize()?;
        Ok(())
    }

    /// Returns an owned [ffi::CudaStreamView] for this stream.
    #[allow(dead_code)]
    pub(crate) fn view(&self) -> UniquePtr<ffi::CudaStreamView> {
        ffi::cuda_stream_view(self.inner())
    }

    /// Get a reference to the underlying FFI stream handle.
    #[allow(dead_code)]
    pub(crate) fn inner(&self) -> &libcudf_sys::ffi::CudaStream {
        self.inner.as_ref().expect("CudaStream should not be null")
    }
}

/// Return a non-null CUDA stream view reference from a cuDF FFI handle.
///
/// cuDF should always return a valid stream view; this surfaces a Rust error if
/// the FFI handle is unexpectedly null.
pub(crate) fn stream_ref(stream: &UniquePtr<ffi::CudaStreamView>) -> Result<&ffi::CudaStreamView> {
    stream
        .as_ref()
        .ok_or(CuDFError::NullHandle("CUDA stream view"))
}

impl Default for CuDFStream {
    fn default() -> Self {
        Self::new()
    }
}
