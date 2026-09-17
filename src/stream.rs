use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use arrow_schema::ArrowError;
use cxx::UniquePtr;
use std::sync::{Arc, OnceLock};

use crate::{CuDFError, Result};
use libcudf_sys::ffi;

/// Default stream to use when no stream is specified.
static DEFAULT_EXECUTION_STREAM: OnceLock<std::result::Result<CuDFStream, String>> =
    OnceLock::new();

/// Return the process-global nonblocking CUDA execution stream.
pub fn global_execution_stream() -> Result<CuDFStream> {
    match DEFAULT_EXECUTION_STREAM.get_or_init(|| {
        CuDFStream::try_with_flags(CuDFStreamFlags::NonBlocking).map_err(|error| error.to_string())
    }) {
        Ok(stream) => Ok(stream.clone()),
        Err(message) => Err(CuDFError::Configuration(message.clone())),
    }
}

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
/// This type owns an opaque C++ `rmm::cuda_stream`. Dropping `CuDFStream`
/// destroys that underlying stream.
///
/// The handle may be shared across host threads. Work enqueued onto the same
/// stream executes in order; sharing the handle does not by itself synchronize
/// access to GPU memory used by those operations.
#[derive(Clone)]
pub struct CuDFStream {
    // Kept alive so the underlying C++ `rmm::cuda_stream` is destroyed on
    // drop. Accessed via `inner()` from within the crate.
    inner: Arc<UniquePtr<libcudf_sys::ffi::CudaStream>>,
}

impl CuDFStream {
    fn try_from_inner(inner: UniquePtr<ffi::CudaStream>) -> Result<Self> {
        if inner.is_null() {
            return Err(CuDFError::NullHandle("CUDA stream"));
        }
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Try to create a stream using the sync-default creation flag.
    pub fn try_new() -> Result<Self> {
        Self::try_from_inner(ffi::cuda_stream_create()?)
    }

    /// Try to create a stream with explicit creation flags.
    pub fn try_with_flags(flags: CuDFStreamFlags) -> Result<Self> {
        Self::try_from_inner(ffi::cuda_stream_create_with_flags(flags.into())?)
    }

    /// Block until all work submitted to this stream has completed.
    pub fn synchronize(&self) -> Result<()> {
        self.inner()?.synchronize()?;
        Ok(())
    }

    /// Return whether both handles own the same CUDA stream.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    /// Returns a non-owning [ffi::CudaStreamView] for this stream.
    ///
    /// # Safety
    ///
    /// The returned view must not outlive `self`.
    #[allow(dead_code)]
    pub(crate) unsafe fn view(&self) -> Result<UniquePtr<ffi::CudaStreamView>> {
        Ok(unsafe { ffi::cuda_stream_view(self.inner()?) })
    }

    /// Get a reference to the underlying FFI stream handle.
    #[allow(dead_code)]
    pub(crate) fn inner(&self) -> Result<&ffi::CudaStream> {
        self.inner
            .as_ref()
            .as_ref()
            .ok_or(CuDFError::NullHandle("CUDA stream"))
    }
}

/// Return the execution stream shared by every GPU column in `batch`.
/// Errors if, for any reason, the stream is not the same for all GPU buffers.
pub fn record_batch_execution_stream(batch: &RecordBatch) -> Result<Option<CuDFStream>> {
    record_batch_execution_stream_from_columns(batch.columns())
}

pub(crate) fn record_batch_execution_stream_from_columns(
    columns: &[ArrayRef],
) -> Result<Option<CuDFStream>> {
    common_execution_stream(
        columns.iter().filter_map(|column| {
            if let Some(column) = column.as_any().downcast_ref::<crate::CuDFColumnView>() {
                Some(column.execution_stream())
            } else {
                column
                    .as_any()
                    .downcast_ref::<crate::CuDFScalar>()
                    .map(crate::CuDFScalar::execution_stream)
            }
        }),
        "record batch columns",
    )
}

/// Return a view of the process-global stream used by high-level cuDF operations.
pub(crate) fn execution_stream() -> Result<UniquePtr<ffi::CudaStreamView>> {
    let stream = global_execution_stream()?;

    unsafe { stream.view() }
}

/// Require two handles to own the same CUDA stream (through shared ownership).
///
/// Equality is based on shared stream identity. This function does not treat
/// separately created streams as equal, even if the caller synchronized them.
pub(crate) fn ensure_same_stream(
    expected: &CuDFStream,
    actual: &CuDFStream,
    context: &str,
) -> Result<()> {
    /// TODO: This is probably a smell. We should introduce a unique ID for streams.
    if expected.ptr_eq(actual) {
        Ok(())
    } else {
        Err(
            ArrowError::InvalidArgumentError(format!("{context} must use the same CUDA stream"))
                .into(),
        )
    }
}

/// Return the common stream when every input refers to the same CUDA stream.
///
/// Returns `None` for no inputs and an error for different stream identities.
///
/// This function does not synchronize streams or establish dependencies
/// between them.
pub(crate) fn common_execution_stream(
    streams: impl IntoIterator<Item = CuDFStream>,
    context: &str,
) -> Result<Option<CuDFStream>> {
    let mut streams = streams.into_iter();
    let Some(stream) = streams.next() else {
        return Ok(None);
    };
    for other in streams {
        ensure_same_stream(&stream, &other, context)?;
    }
    Ok(Some(stream))
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array};
    use arrow_schema::{DataType, Field, Schema};

    #[test]
    fn execution_stream_is_not_a_default_stream(
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let stream = execution_stream()?;
        let stream = stream_ref(&stream)?;

        assert!(!stream.is_default());
        assert!(!stream.is_per_thread_default());
        Ok(())
    }

    #[test]
    fn record_batch_keeps_its_execution_stream(
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let first = CuDFStream::try_with_flags(CuDFStreamFlags::NonBlocking)?;
        let second = CuDFStream::try_with_flags(CuDFStreamFlags::NonBlocking)?;
        assert!(!first.ptr_eq(&second));

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let column = crate::CuDFColumn::try_from_arrow_host_on_stream(
            &Int32Array::from(vec![1, 2]),
            &second,
        )?;
        let batch = crate::record_batch_with_schema(
            vec![Arc::new(column.into_view()) as ArrayRef],
            &schema,
            2,
        )?;

        let attached = record_batch_execution_stream(&batch)?.expect("GPU batch has a stream");
        assert!(attached.ptr_eq(&second));
        second.synchronize()?;
        Ok(())
    }

    #[test]
    fn record_batch_rejects_columns_from_different_streams(
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let first = CuDFStream::try_with_flags(CuDFStreamFlags::NonBlocking)?;
        let second = CuDFStream::try_with_flags(CuDFStreamFlags::NonBlocking)?;
        let first_column = crate::CuDFColumn::try_from_arrow_host_on_stream(
            &Int32Array::from(vec![1, 2]),
            &first,
        )?;
        let second_column = crate::CuDFColumn::try_from_arrow_host_on_stream(
            &Int32Array::from(vec![3, 4]),
            &second,
        )?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("first", DataType::Int32, false),
            Field::new("second", DataType::Int32, false),
        ]));

        let error = crate::record_batch_with_schema(
            vec![
                Arc::new(first_column.into_view()) as ArrayRef,
                Arc::new(second_column.into_view()) as ArrayRef,
            ],
            &schema,
            2,
        )
        .expect_err("mixed-stream batch must be rejected");

        assert!(error.to_string().contains("same CUDA stream"));
        first.synchronize()?;
        second.synchronize()?;
        Ok(())
    }
}
