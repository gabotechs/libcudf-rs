//! Pinned host memory utilities for accelerating Arrow → GPU uploads.
//!
//! When the host source of a `cudaMemcpyAsync` is pageable memory (e.g. a Rust
//! `Vec` or an arrow-rs default-allocator buffer), the CUDA driver must first
//! stage the data through a single device-wide pinned staging buffer before
//! kicking off the DMA. That staging step is synchronous. All `cudaMemcpyAsync`
//! calls on a stream will serialize.
//!
//! When the source is page-locked ("pinned") memory allocated via
//! `cudaMallocHost`, the driver can DMA directly from the source and the call
//! is fully asynchronous.
use crate::config::ensure_pools_configured;
use crate::errors::Result;
use crate::stream::stream_ref;
use arrow::alloc::Allocation;
use arrow::array::{make_array, ArrayData, ArrayDataBuilder, RecordBatch};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
use cxx::UniquePtr;
use libcudf_sys::ffi::{self, get_pinned_memory_resource, HostDeviceAsyncResourceRef};
use std::ptr::NonNull;
use std::sync::{Arc, OnceLock};

/// Process-global handle to cuDF's pinned MR. Lazily initialized once;
/// thereafter every alloc/dealloc reads through the same `&'static` handle.
/// The underlying cuDF MR (a `pinned_pool_with_fallback_memory_resource`
/// backed by `rmm::mr::pool_memory_resource`) is itself thread-safe.
fn pinned_mr() -> &'static HostDeviceAsyncResourceRef {
    static MR: OnceLock<UniquePtr<HostDeviceAsyncResourceRef>> = OnceLock::new();
    MR.get_or_init(get_pinned_memory_resource)
        .as_ref()
        .expect("pinned MR is null")
}

/// RAII wrapper for a single pinned host allocation drawn from cuDF's pinned
/// memory resource. The pool is process-global and shared with cuDF's own
/// pinned-memory consumers (e.g. the download path).
pub struct PinnedHostBuffer {
    ptr: *mut u8,
    bytes: usize,
}

// SAFETY: A pinned host allocation is plain memory addressable by both the
// host and the device. There is no thread-affinity on the CUDA side, so the
// buffer can be moved across threads.
unsafe impl Send for PinnedHostBuffer {}
unsafe impl Sync for PinnedHostBuffer {}

impl PinnedHostBuffer {
    /// Allocate `bytes` of pinned host memory from cuDF's pinned MR.
    fn new(bytes: usize) -> Result<Self> {
        let ptr = pinned_mr().allocate_sync(bytes)? as *mut u8;
        Ok(Self { ptr, bytes })
    }

    /// Raw pointer to the start of the allocation.
    fn as_ptr(&self) -> *mut u8 {
        self.ptr
    }
}

impl Drop for PinnedHostBuffer {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            pinned_mr().deallocate_sync(self.ptr as usize, self.bytes);
        }
    }
}

/// Block until all GPU work submitted to cuDF's default stream has
/// completed.
///
/// Required after issuing an asynchronous upload from a pinned source if the
/// source is about to be dropped, since `cudaMemcpyAsync` returns before the
/// DMA has finished and the pinned buffer must outlive the transfer.
pub fn synchronize_default_stream() -> Result<()> {
    let stream = ffi::get_default_stream();
    stream_ref(&stream)?.synchronize()?;
    Ok(())
}

/// Return a copy of `batch` whose underlying buffers all live in pinned
/// (page-locked) host memory.
///
/// The schema, lengths, offsets, and null counts of every column are
/// preserved exactly; only the host-side storage of each leaf
/// [`Buffer`] is replaced with a pinned-backed copy.
///
/// Empty buffers are passed through unchanged because `cudaMallocHost(0)` is
/// not portable and a zero-byte buffer has no data to DMA.
pub fn pin_record_batch(batch: RecordBatch) -> Result<RecordBatch> {
    ensure_pools_configured();
    let schema = batch.schema();
    let arrays = batch
        .columns()
        .iter()
        .map(|arr| pin_array_data(arr.to_data()).map(make_array))
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema, arrays)?)
}

fn pin_array_data(data: ArrayData) -> Result<ArrayData> {
    let buffers = data
        .buffers()
        .iter()
        .map(pin_buffer)
        .collect::<Result<Vec<_>>>()?;

    let children = data
        .child_data()
        .iter()
        .cloned()
        .map(pin_array_data)
        .collect::<Result<Vec<_>>>()?;

    let mut builder = ArrayDataBuilder::new(data.data_type().clone())
        .len(data.len())
        .offset(data.offset())
        .buffers(buffers)
        .child_data(children);

    // The null mask is small (1 bit per row), but leaving it pageable means
    // every nullable column still pays the per-call staging cost (~30-60 µs)
    // on its `cudaMemcpyAsync`. Pinning it makes the upload uniformly async.
    if let Some(nulls) = data.nulls() {
        builder = builder.nulls(Some(pin_null_buffer(nulls)?));
    }

    // SAFETY: only the storage of each leaf buffer is replaced; data type,
    // lengths, offsets, and null counts are preserved. The new ArrayData is
    // structurally identical to the input.
    Ok(unsafe { builder.build_unchecked() })
}

fn pin_null_buffer(nulls: &NullBuffer) -> Result<NullBuffer> {
    let bool_buf = nulls.inner();
    let pinned = pin_buffer(bool_buf.inner())?;
    let new_bool = BooleanBuffer::new(pinned, bool_buf.offset(), bool_buf.len());
    // SAFETY: `pin_buffer` copies the underlying bytes verbatim, so the bit
    // pattern (and therefore the null count) is preserved.
    Ok(unsafe { NullBuffer::new_unchecked(new_bool, nulls.null_count()) })
}

fn pin_buffer(buf: &Buffer) -> Result<Buffer> {
    let bytes = buf.len();
    if bytes == 0 {
        return Ok(buf.clone());
    }

    let pinned = Arc::new(PinnedHostBuffer::new(bytes)?);
    let dst = pinned.as_ptr();
    // SAFETY: `pinned` was just allocated with at least `bytes` capacity,
    // `buf.as_ptr()` is valid for `bytes` reads, and the regions do not
    // overlap (different allocations).
    unsafe {
        std::ptr::copy_nonoverlapping(buf.as_ptr(), dst, bytes);
        Ok(Buffer::from_custom_allocation(
            NonNull::new(dst).expect("pinned allocation pointer is non-null"),
            bytes,
            pinned as Arc<dyn Allocation>,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn pinned_host_buffer_round_trip() -> Result<()> {
        let buf = PinnedHostBuffer::new(64)?;
        // SAFETY: we own the allocation and it has 64 bytes of capacity.
        unsafe {
            let slice = std::slice::from_raw_parts_mut(buf.as_ptr(), 64);
            slice.fill(0xAB);
            assert!(slice.iter().all(|b| *b == 0xAB));
        }
        Ok(())
    }

    #[test]
    fn pin_record_batch_preserves_primitive_data() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let values: Vec<i64> = (0..1024).collect();
        let arr = Int64Array::from(values.clone());
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)])?;

        let pinned = pin_record_batch(batch)?;
        let out = pinned
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        assert_eq!(out.len(), values.len());
        for (i, expected) in values.iter().enumerate() {
            assert_eq!(out.value(i), *expected);
        }
        Ok(())
    }

    #[test]
    fn pin_record_batch_preserves_variable_width_data() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let arr = StringArray::from(vec![Some("alpha"), None, Some("beta"), Some("gamma")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)])?;

        let pinned = pin_record_batch(batch)?;
        let out = pinned
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");
        assert_eq!(out.len(), 4);
        assert_eq!(out.value(0), "alpha");
        assert!(out.is_null(1));
        assert_eq!(out.value(2), "beta");
        assert_eq!(out.value(3), "gamma");
        Ok(())
    }

    /// `PinnedHostBuffer::Drop` must be safe to run during unwinding — a
    /// user `panic!` while a pinned batch is in flight should propagate
    /// cleanly without double-panic.
    #[test]
    fn drop_during_unwinding_does_not_double_panic() {
        let result = std::panic::catch_unwind(|| {
            let _buf = PinnedHostBuffer::new(1024).expect("alloc");
            panic!("simulated user panic");
        });
        assert!(
            result.is_err(),
            "outer panic should propagate to catch_unwind"
        );
    }
}
