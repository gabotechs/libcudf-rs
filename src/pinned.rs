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
use arrow::alloc::Allocation;
use arrow::array::{make_array, ArrayData, ArrayDataBuilder, RecordBatch};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
use cxx::UniquePtr;
use libcudf_sys::ffi::{
    cuda_default_stream_synchronize, pinned_host_alloc, pinned_host_free, PinnedHostAlloc,
};
use std::cell::RefCell;
use std::ptr::NonNull;
use std::sync::Arc;

use crate::errors::Result;

/// Internal owner for a single pinned allocation via `cudaMallocHost`. The
/// allocation is freed automatically on drop.
struct PinnedAllocOwner {
    inner: Option<UniquePtr<PinnedHostAlloc>>,
}

// SAFETY: A pinned host allocation is plain memory addressable by both the
// host and the device. There is no thread-affinity on the CUDA side, so the
// owner can be moved across threads.
unsafe impl Send for PinnedAllocOwner {}
unsafe impl Sync for PinnedAllocOwner {}

impl PinnedAllocOwner {
    fn new(bytes: usize) -> Result<Self> {
        Ok(Self {
            inner: Some(pinned_host_alloc(bytes)?),
        })
    }

    fn capacity(&self) -> usize {
        self.inner_ref().len()
    }

    fn data_ptr(&self) -> *mut u8 {
        self.inner_ref().data() as *mut u8
    }

    fn inner_ref(&self) -> &PinnedHostAlloc {
        self.inner
            .as_ref()
            .and_then(|i| i.as_ref())
            .expect("PinnedHostAlloc should not be null")
    }
}

impl Drop for PinnedAllocOwner {
    fn drop(&mut self) {
        let Some(alloc) = self.inner.take() else { return };
        if let Err(err) = pinned_host_free(alloc) {
            if std::thread::panicking() {
                // Already unwinding — surface the failure but don't abort by
                // double-panicking.
                eprintln!("libcudf_rs: cudaFreeHost failed during unwinding: {err}");
            } else {
                panic!("cudaFreeHost failed: {err}");
            }
        }
    }
}

/// Wrapper for [`PinnedAllocOwner`] used to pool / re-use allocations.
pub struct PinnedHostBuffer {
    inner: Option<PinnedAllocOwner>,
    requested_bytes: usize,
}

thread_local! {
    /// Thread-local pool of pinned host allocations available for reuse.
    ///
    /// `cudaMallocHost` / `cudaFreeHost` each take hundreds of microseconds,
    /// so allocations are recycled here instead of being freed on drop. On a
    /// `new(bytes)` request we linearly pick the smallest pooled allocation
    /// with capacity >= `bytes`; the pool stays small enough that the linear
    /// scan is fine. `cudaFreeHost` only runs when the pool itself drops at
    /// thread exit (see [`PinnedAllocOwner::drop`]).
    ///
    /// # Why thread-local instead of a global pool
    ///
    /// 1. No locking on the hot path (~20K allocs per aggregate query). A
    ///    global pool would need a `Mutex<Vec<...>>` on every alloc/free.
    /// 2. NUMA locality — the memory stays close to the CPU that pinned it.
    ///
    /// Tradeoff: a buffer allocated on Thread A and dropped on Thread B
    /// (e.g. across an `.await` where a tokio task hopped workers) ends up
    /// in B's pool, not A's. That's an efficiency loss, not a correctness
    /// bug, and our hot path (`pin_record_batch` → `from_arrow_host` →
    /// drop) is synchronous within a single closure so it doesn't trip that
    /// case in practice.
    ///
    /// # Why `RefCell` is sufficient (no `Mutex`)
    ///
    /// `thread_local!` gives each thread its own `RefCell`, and the only
    /// way to reach it — `PINNED_POOL.with(|cell| ...)` — hands out a borrow
    /// whose lifetime is tied to the closure; that borrow can't be returned,
    /// stored, or `Send`-ed to another thread. So no two threads ever hold
    /// a reference to the same `RefCell`, and the runtime borrow check only
    /// has to guard same-thread reentrancy. (A plain `static RefCell<...>`
    /// wouldn't compile because `RefCell` is `!Sync`; `thread_local!` is
    /// the escape hatch.)
    ///
    /// # Why we don't reuse RMM's pool
    ///
    /// RMM ships `rmm::mr::pinned_host_memory_resource`; combined with
    /// `rmm::mr::pool_memory_resource` it would give equivalent pooling for
    /// free. We deliberately don't use it: exposing an RMM resource through
    /// cxx is several files of glue for one call site, and a plain
    /// `Vec<PinnedAllocOwner>` is easy to read, easy to test, and ~50 LOC.
    /// Worth revisiting if more pinned-memory consumers land in this crate.
    ///
    /// Unrelated to [`crate::PinnedPoolConfig`], which configures cuDF's
    /// *internal* pinned pool used for the download path.
    static PINNED_POOL: RefCell<Vec<PinnedAllocOwner>> =
        const { RefCell::new(Vec::new()) };
}

#[cfg(test)]
fn pool_len() -> usize {
    PINNED_POOL.with(|p| p.borrow().len())
}

/// Drop every cached allocation on the current thread. Drains via
/// [`PinnedAllocOwner::drop`], so any `cudaFreeHost` failure becomes a panic
/// here. Test-only — production code never needs to drain explicitly.
#[cfg(test)]
fn drain_pool() {
    PINNED_POOL.with(|p| p.borrow_mut().clear());
}

impl PinnedHostBuffer {
    /// Allocate `bytes` of pinned host memory, reusing a pooled buffer if one
    /// of sufficient capacity is available on the current thread.
    pub fn new(bytes: usize) -> Result<Self> {
        let pooled = PINNED_POOL.with(|pool| {
            let mut pool = pool.borrow_mut();
            // Pick the smallest pooled buffer with capacity >= requested.
            let pos = pool
                .iter()
                .enumerate()
                .filter(|(_, owner)| owner.capacity() >= bytes)
                .min_by_key(|(_, owner)| owner.capacity())
                .map(|(i, _)| i);
            pos.map(|i| pool.swap_remove(i))
        });
        let inner = match pooled {
            Some(owner) => owner,
            None => PinnedAllocOwner::new(bytes)?,
        };
        Ok(Self {
            inner: Some(inner),
            requested_bytes: bytes,
        })
    }

    /// Number of bytes the caller requested. May be less than the underlying
    /// allocation's capacity if it came from the pool.
    pub fn len(&self) -> usize {
        self.requested_bytes
    }

    /// Whether the requested allocation is zero-sized.
    pub fn is_empty(&self) -> bool {
        self.requested_bytes == 0
    }

    /// Raw pointer to the start of the allocation.
    pub fn as_ptr(&self) -> *mut u8 {
        self.inner
            .as_ref()
            .expect("PinnedHostBuffer must own an allocation")
            .data_ptr()
    }
}

impl Drop for PinnedHostBuffer {
    fn drop(&mut self) {
        if let Some(owner) = self.inner.take() {
            // Return to the thread-local pool for reuse rather than freeing.
            // The actual `cudaFreeHost` happens when the pool itself drops at
            // thread exit, via `PinnedAllocOwner::drop`.
            PINNED_POOL.with(|pool| pool.borrow_mut().push(owner));
        }
    }
}

/// Block until all GPU work submitted to the CUDA default stream has
/// completed.
///
/// Required after issuing an asynchronous upload from a pinned source if the
/// source is about to be dropped, since `cudaMemcpyAsync` returns before the
/// DMA has finished and the pinned buffer must outlive the transfer.
pub fn synchronize_default_stream() -> Result<()> {
    cuda_default_stream_synchronize()?;
    Ok(())
}

/// Return a copy of `batch` whose underlying buffers all live in pinned
/// (page-locked) host memory.
///
/// The schema, lengths, offsets, and null counts of every column are
/// preserved exactly; only the host-side storage of each leaf
/// [`arrow::buffer::Buffer`] is replaced with a pinned-backed copy.
///
/// Empty buffers are passed through unchanged because `cudaMallocHost(0)` is
/// not portable and a zero-byte buffer has no data to DMA.
pub fn pin_record_batch(batch: RecordBatch) -> Result<RecordBatch> {
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
        assert_eq!(buf.len(), 64);
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

    /// Dropping a [`PinnedHostBuffer`] should return its allocation to the
    /// thread-local pool so the next allocation of the same size reuses it
    /// instead of calling `cudaMallocHost` again.
    #[test]
    fn drop_returns_buffer_to_pool() -> Result<()> {
        drain_pool();

        let buf = PinnedHostBuffer::new(2048)?;
        let ptr_before = buf.as_ptr() as usize;
        assert_eq!(pool_len(), 0);
        drop(buf);
        assert_eq!(pool_len(), 1, "drop should push the allocation into the pool");

        let buf2 = PinnedHostBuffer::new(2048)?;
        assert_eq!(buf2.as_ptr() as usize, ptr_before, "pool should return the same allocation");
        assert_eq!(pool_len(), 0, "reuse should remove the allocation from the pool");
        Ok(())
    }

    /// When a request can be served by multiple pooled allocations, the pool
    /// should return the smallest one whose capacity is at least the request,
    /// to avoid permanently inflating the working set.
    #[test]
    fn pool_picks_smallest_sufficient() -> Result<()> {
        drain_pool();

        let small = PinnedHostBuffer::new(1024)?;
        let medium = PinnedHostBuffer::new(4096)?;
        let large = PinnedHostBuffer::new(16_384)?;
        let small_ptr = small.as_ptr() as usize;
        let medium_ptr = medium.as_ptr() as usize;
        let large_ptr = large.as_ptr() as usize;
        drop(small);
        drop(medium);
        drop(large);

        // 3 KiB request — the 4 KiB allocation is the smallest sufficient.
        let pick = PinnedHostBuffer::new(3000)?;
        let pick_ptr = pick.as_ptr() as usize;
        assert_eq!(pick_ptr, medium_ptr, "expected the 4 KiB pooled allocation");
        assert_ne!(pick_ptr, small_ptr);
        assert_ne!(pick_ptr, large_ptr);
        Ok(())
    }

    /// `PinnedHostBuffer::Drop` must be safe to run during unwinding — it
    /// returns to the pool without panicking, so a user `panic!` while a
    /// pinned batch is in flight should be caught cleanly.
    #[test]
    fn drop_during_unwinding_does_not_double_panic() {
        let result = std::panic::catch_unwind(|| {
            let _buf = PinnedHostBuffer::new(1024).expect("alloc");
            panic!("simulated user panic");
        });
        assert!(result.is_err(), "outer panic should propagate to catch_unwind");
    }
}
