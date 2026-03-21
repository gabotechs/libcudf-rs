# Fix: Implicit GPU→CPU Copies via Arrow Metrics

## Problem

Every time a GPU operator (filter, projection, coalesce) emitted a batch, DataFusion was
silently copying every column from GPU VRAM back to CPU RAM just to record a metrics
number. This happened on **every batch**, regardless of whether metrics were being read.

### Root cause

DataFusion v51 changed `BaselineMetrics::record_poll` to also call
`get_record_batch_memory_size` for spill-size accounting. That function walks every column
in the batch and calls `Array::to_data()` to measure how many bytes it holds.

`CuDFColumnView` implements Arrow's `Array` trait. Its `to_data()` implementation performs
a full `cudaMemcpy(DeviceToHost)` to copy the column data to CPU so it can be wrapped in
an `ArrayData`. That copy was completely invisible — it looked like a routine metrics call.

### Call stack (before)

```
BaselineMetrics::record_poll(poll)
  └─ get_record_batch_memory_size(batch)
       └─ for each column: Array::to_data()
            └─ CuDFColumnView::to_data()
                 └─ self.to_arrow_host()   ← cudaMemcpy(DeviceToHost) per column
```

This fired in three operators on every output batch:

| Operator | File |
|---|---|
| `CuDFFilterExec` | `physical/filter.rs` |
| `CuDFCoalesceBatchesExec` | `physical/coalesce_batches.rs` |
| `CuDFProjectionExec` | `physical/projection.rs` |

The memory size being recorded was also semantically wrong: GPU data is in VRAM and is
never subject to DataFusion's host spill mechanism, so recording phantom byte counts could
have caused the memory manager to throttle based on memory that isn't consuming host RAM.

---

## Changes

### 1. `src/column_view.rs` — `debug_assert` in `to_data()`

**Before:**
```rust
fn to_data(&self) -> ArrayData {
    // WARNING: This performs a full GPU to CPU data transfer.
    self.to_arrow_host()
        .expect("Failed to convert GPU column to host Arrow")
        .to_data()
}
```

**After:**
```rust
fn to_data(&self) -> ArrayData {
    // In debug/test builds this panics so implicit GPU->CPU copies are easily
    // caught.
    //
    // Call to_arrow_host() explicitly rather than this method to separate implicit
    // and explicit transfers.
    debug_assert!(
        false,
        "CuDFColumnView::to_data(), implicit GPU→CPU copy detected. \
         Call to_arrow_host() explicitly."
    );
    self.to_arrow_host()
        .expect("Failed to convert GPU column to host Arrow")
        .to_data()
}
```

`debug_assert!(false, ...)` compiles to nothing in release builds (zero overhead), but
panics immediately in `cargo test` and debug builds. Any future code path that reaches
`to_data()` on a GPU column will surface as a test failure rather than a silent perf
regression.

`to_data()` is kept functional (not removed) because `CuDFUnloadExec` uses `to_arrow_host()`
directly — it never goes through `to_data()`. If some unforeseen path does reach it in
production, it degrades gracefully rather than crashing.

---

### 2. `libcudf-datafusion/src/physical/mod.rs` — `record_gpu_poll`

**Before:** each operator had a TODO comment and called `record_poll` directly.

**After:** a shared helper replaces `record_poll` for all GPU operators:

```rust
/// GPU-safe replacement for [`BaselineMetrics::record_poll`].
///
/// `record_poll` internally calls `get_record_batch_memory_size` → `Array::to_data()`,
/// which for `CuDFColumnView` triggers a full GPU→CPU copy just to measure spill size.
/// GPU data is never subject to DataFusion's host spill mechanism, so only the row
/// count is recorded.
pub(crate) fn record_gpu_poll(
    metrics: &BaselineMetrics,
    poll: Poll<Option<Result<RecordBatch, DataFusionError>>>,
) -> Poll<Option<Result<RecordBatch, DataFusionError>>> {
    if let Poll::Ready(Some(Ok(ref batch))) = poll {
        metrics.output_rows().add(batch.num_rows());
    }
    poll
}
```

Recording only `output_rows` (a stored `usize` field) involves no FFI and no GPU access.

---

### 3. Three operator streams — replace `record_poll` with `record_gpu_poll`

**Before** (same pattern in all three):
```rust
// TODO(#21): record_poll triggers Array::to_data() -> GPU->CPU for CuDFColumnView.
// Replace with record_output(batch.num_rows()) once #21 is addressed.
self.baseline_metrics.record_poll(poll)
```

**After:**
```rust
record_gpu_poll(&self.baseline_metrics, poll)
```

The TODO comments are resolved and removed.

---

## How to detect regressions

Adding a new GPU operator that calls `record_poll` will cause test failures because
`to_data()` fires `debug_assert!(false)`. The fix is always the same: use `record_gpu_poll`
instead of `record_poll` in any operator stream that handles `CuDFColumnView` batches.

The explicit GPU→CPU boundary remains `CuDFUnloadExec`, which calls `to_arrow_host()`
directly and is the only place a full column copy is expected and intentional.
