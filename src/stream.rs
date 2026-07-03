use cxx::UniquePtr;

use crate::{CuDFError, Result};
use libcudf_sys::ffi;

/// Stream creation flags for CUDA stream-backed cuDF execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum CuDFStreamFlags {
    /// Create an owned stream with CUDA's default synchronization behavior.
    ///
    /// This is not the legacy default stream itself, it is a separate stream
    /// that synchronizes with default-stream work according to CUDA rules.
    SyncDefault = 0,
    /// Create a non-blocking stream that does not synchronize with the default stream.
    NonBlocking = 1,
}

impl From<CuDFStreamFlags> for u32 {
    fn from(value: CuDFStreamFlags) -> Self {
        value as u32
    }
}

/// Owned CUDA stream used by cuDF operations.
///
/// Most callers should use [`CuDFExecutionContext`](crate::CuDFExecutionContext)
/// to submit work. Use `CuDFStream` directly when constructing a context from a
/// specific owned stream.
///
/// The handle may be shared across host threads. Work enqueued onto the same
/// stream executes in order; sharing the handle does not by itself synchronize
/// access to GPU memory used by those operations.
pub struct CuDFStream {
    inner: UniquePtr<libcudf_sys::ffi::CudaStream>,
    device_id: i32,
}

impl CuDFStream {
    /// Creates an owned stream with [`CuDFStreamFlags::SyncDefault`].
    ///
    /// # Errors
    ///
    /// Returns an error if CUDA cannot create the stream or report the current
    /// device.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use libcudf_rs::{CuDFExecutionContext, CuDFStream};
    ///
    /// let stream = CuDFStream::try_new()?;
    /// let ctx = CuDFExecutionContext::try_from_stream(stream)?;
    /// ctx.synchronize()?;
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn try_new() -> Result<Self> {
        Ok(Self {
            inner: ffi::cuda_stream_create()?,
            device_id: ffi::cuda_get_device()?,
        })
    }

    /// Creates an owned stream with explicit CUDA stream creation flags.
    ///
    /// # Errors
    ///
    /// Returns an error if CUDA cannot create the stream or report the current
    /// device.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use libcudf_rs::{CuDFExecutionContext, CuDFStream, CuDFStreamFlags};
    ///
    /// let stream = CuDFStream::try_with_flags(CuDFStreamFlags::NonBlocking)?;
    /// let ctx = CuDFExecutionContext::try_from_stream(stream)?;
    /// ctx.synchronize()?;
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn try_with_flags(flags: CuDFStreamFlags) -> Result<Self> {
        Ok(Self {
            inner: ffi::cuda_stream_create_with_flags(flags.into())?,
            device_id: ffi::cuda_get_device()?,
        })
    }

    /// Blocks until all work submitted to this stream has completed.
    ///
    /// # Errors
    ///
    /// Returns an error if CUDA stream synchronization fails.
    pub fn synchronize(&self) -> Result<()> {
        self.inner()?.synchronize()?;
        Ok(())
    }

    /// Returns an owned cuDF stream view for this stream.
    pub(crate) fn view(&self) -> Result<UniquePtr<ffi::CudaStreamView>> {
        Ok(ffi::cuda_stream_view(self.inner()?))
    }

    /// Returns the CUDA device that was current when this stream was created.
    pub(crate) fn device_id(&self) -> i32 {
        self.device_id
    }

    fn inner(&self) -> Result<&ffi::CudaStream> {
        self.inner
            .as_ref()
            .ok_or(CuDFError::NullHandle("CUDA stream"))
    }
}
