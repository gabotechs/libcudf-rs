# CUDA Streams Implementation

Date: 2026-05-08

Companion to `docs/cuda_streams.md`. This document describes how CUDA streams
are wired through this branch: the optimizer rule, query runtime state, FFI
bindings, high-level Rust API, and DataFusion physical operators.

## Goals

The stream implementation has three goals:

1. Keep the default behavior conservative. When streams are disabled, cuDF GPU
   work still runs as one GPU partition on the default stream.
2. When streams are enabled, let independent DataFusion GPU partitions enqueue
   work on independent non-blocking CUDA streams.
3. Preserve CUDA ordering inside one GPU pipeline. A load, filter, projection,
   aggregate, sort, and unload in the same GPU segment use the same stream for
   a given DataFusion partition.

The implementation deliberately avoids thread-local stream state and global
mutable stream maps. Streams live in the query `TaskContext`, and operators pass
stream handles explicitly to the cuDF wrappers that need them.

## Configuration

The user-facing switch is `CuDFConfig::cuda_streams` in:

- `libcudf-datafusion/src/planner/config.rs`

Defaults:

| setting | default | meaning |
|---|---:|---|
| `cudf.cuda_streams` | `false` | use the default stream and collapse the GPU subplan to one partition |
| `cudf.aggregate_chunk_target_bytes` | `256 MiB` | per-aggregate-stream pending input target before flushing |
| `cudf.pinned_input` | `true` | stage host Arrow input into pinned memory before H2D upload |

The benchmark harness reads `CUDF_BENCH_CUDA_STREAMS`. The TPC-H correctness
tests read `CUDF_TEST_CUDA_STREAMS` and `CUDF_TEST_TARGET_PARTITIONS`. Runnable
commands are collected in the appendices.

## Planner Behavior

Streams affect planning in two places.

### Session State Extension

`SessionStateBuilderExt::with_cudf_planner()` installs:

1. `RescaleLeafsRule`
2. `HostToCuDFRule`

When streams are disabled, the extension temporarily sets
`datafusion.execution.target_partitions = 1` before the cuDF optimizer rules
run. This keeps the GPU subplan single-partitioned. `RescaleLeafsRule` then
fans leaf nodes back out to the original target partition count and inserts
`CoalescePartitionsExec` where needed, so CPU-side scan parallelism is retained.

When streams are enabled, the extension leaves `target_partitions` unchanged.
That means DataFusion can execute the GPU subplan with multiple output
partitions, and each partition can get its own CUDA stream.

This is important for interpreting benchmarks: streams-on changes both the CUDA
stream choice and the GPU partitioning. It is not only a stream flag.

### Host To CuDF Rule

`HostToCuDFRule` rewrites supported physical operators to cuDF operators:

| DataFusion operator | cuDF operator |
|---|---|
| `FilterExec` | `CuDFFilterExec` |
| `ProjectionExec` | `CuDFProjectionExec` |
| `AggregateExec` | `CuDFAggregateExec` |
| `SortExec` | `CuDFSortExec` |
| supported hash joins | cuDF hash join implementation |

The rule inserts boundary nodes:

| boundary | node |
|---|---|
| host Arrow to GPU cuDF | `CuDFLoadExec` |
| GPU cuDF to host Arrow | `CuDFUnloadExec` |

For streams, `CuDFLoadExec` is constructed with
`preserve_input_partitioning = cudf_config.cuda_streams`.

| streams | `CuDFLoadExec` partition behavior |
|---|---|
| off | one GPU output partition uploads all input partitions |
| on | output partition `p` uploads input partition `p` |

## Segment IDs

After the cuDF rewrite, `assign_segment_ids()` walks the physical plan and tags
each contiguous stream-eligible GPU component with a `segment_id`.

Stream-eligible nodes:

- `CuDFLoadExec`
- `CuDFFilterExec`
- `CuDFProjectionExec`
- `CuDFAggregateExec`
- `CuDFSortExec`
- `CuDFUnloadExec`

`segment_id = 0` is a sentinel meaning "use the default stream". Nonzero IDs
mean "look up the stream for this GPU segment and DataFusion partition".

Segments start at `CuDFLoadExec` and extend upward through contiguous cuDF
operators. If a component contains a cuDF node that is not in the allowlist, the
whole component keeps `segment_id = 0`. Hash join is the main example today:
it is converted to cuDF where supported, but it is not currently stream-aware.

This allowlist is a correctness guard. It prevents a stream-enabled plan from
accidentally running an operator on a private stream when that operator still
contains default-stream assumptions.

## Query Runtime State

Runtime stream state lives in:

- `libcudf-datafusion/src/task_context.rs`
- `libcudf-datafusion/src/cudf_ext.rs`
- `libcudf-datafusion/src/stream_source.rs`

`CuDFTaskContext` stores:

```rust
HashMap<(segment_id, partition), Arc<CuDFStream>>
```

The key is the pair of:

- GPU segment ID assigned by the optimizer
- DataFusion physical partition ID passed to `ExecutionPlan::execute`

`TaskContext::with_cudf_task_context()` installs a fresh `CuDFTaskContext`
extension for a query run. This is required for stream-aware execution because
operators look up streams through the `TaskContext`.

`CuDFStreamSource` allocates `CuDFStreamFlags::NonBlocking` streams. It is stored
inside `CuDFConfig` as private runtime wiring, not as a user-facing config
option.

The ownership model is:

1. `CuDFLoadExec` allocates or reuses the stream for `(segment_id, partition)`.
2. Downstream cuDF operators in the same segment look up that stream.
3. `CuDFUnloadExec` removes the stream entry when the partition stream ends or
   is dropped.

The stream handle is an `Arc<CuDFStream>`. CUDA ordering comes from using the
same CUDA stream for dependent GPU operations, not from locking the `Arc`.

## FFI Layer

The thin FFI layer lives in `libcudf-sys`. It should remain an upstream-shaped
bridge over cuDF/RMM, not an ergonomic API.

Stream-related files:

- `libcudf-sys/src/stream.h`
- `libcudf-sys/src/stream.cpp`
- `libcudf-sys/src/lib.rs`

The FFI layer exposes:

| wrapper | upstream concept |
|---|---|
| `CudaStream` | owning `rmm::cuda_stream` |
| `CudaStreamView` | non-owning `rmm::cuda_stream_view` |
| `cuda_stream_create()` | construct with sync-default flags |
| `cuda_stream_create_with_flags()` | construct with explicit RMM flags |
| `cuda_stream_view()` | borrow an owning stream as a stream view |
| `get_default_stream()` | cuDF default stream |
| `is_ptds_enabled()` | cuDF PTDS setting |

The stream flag values are guarded by C++ `static_assert`s against RMM:

| Rust flag | raw value | RMM flag |
|---|---:|---|
| `SyncDefault` | `0` | `rmm::cuda_stream::flags::sync_default` |
| `NonBlocking` | `1` | `rmm::cuda_stream::flags::non_blocking` |

Stream-aware cuDF bindings include `_on` variants for the operations that cuDF
can execute on an explicit stream:

- groupby aggregation
- boolean mask / `copy_if`
- binary operations
- concatenate table and columns
- sort / stable sorted order / gather
- cast
- Arrow import/export through explicit `CudaStreamView` and device resource
  references

The sys layer does not decide which stream to use. It only exposes the upstream
stream parameter where cuDF has one.

## High-Level libcudf-rs API

The safe Rust layer exposes ergonomic stream-aware variants.

Representative APIs:

| default API | stream API |
|---|---|
| `CuDFTable::from_arrow_host` | `CuDFTable::from_arrow_host_on` |
| `CuDFColumn::from_arrow_host` | `CuDFColumn::from_arrow_host_on` |
| `CuDFTableView::to_arrow_host` | `CuDFTableView::to_arrow_host_on` |
| `CuDFTable::concat` | `CuDFTable::concat_on` |
| `CuDFColumn::concat` | `CuDFColumn::concat_on` |
| `CuDFGroupBy::aggregate` | `CuDFGroupBy::aggregate_on` |
| `apply_boolean_mask` | `apply_boolean_mask_on` |
| `cudf_binary_op` | `cudf_binary_op_on` |
| `sort` | `sort_on` |
| `stable_sorted_order` | `stable_sorted_order_on` |
| `gather` | `gather_on` |
| `cast` | `cast_on` |

Most DataFusion operators carry `Option<Arc<CuDFStream>>` and choose the `_on`
variant when the option is present.

## Expression Plumbing

Expressions are converted once from DataFusion physical expressions to cuDF
physical expressions, then cloned with a stream at execution time.

`expr_with_stream()` walks an expression tree and replaces `CuDFBinaryExpr`
nodes with clones that hold the partition stream. This matters because binary
expressions are used in filters, projections, and aggregate arguments.

There is no thread-local current stream. The stream is explicit:

1. Operator looks up `Option<Arc<CuDFStream>>`.
2. Operator calls `expr_with_stream()`.
3. `CuDFBinaryExpr::evaluate()` dispatches to `cudf_binary_op_on()` or
   `decimal_div_on()` when a stream exists.

## Physical Operator Flow

### Load

`CuDFLoadExec` is the bottom of a GPU segment.

Responsibilities:

- validate the requested DataFusion partition
- allocate or reuse the CUDA stream for `(segment_id, partition)`
- convert host Arrow `RecordBatch` values into cuDF-backed arrays
- optionally stage host input through pinned memory

When `pinned_input = true`, the load path synchronizes the relevant stream after
`from_arrow_host_on()`. That is required because the pinned host batch must
outlive the asynchronous H2D copy. It also means the upload path is not a fully
deferred fire-and-forget enqueue.

### Filter

`CuDFFilterExec` looks up the segment stream and clones its predicate with that
stream. Predicate evaluation can enqueue binary operations on the stream. The
filter then calls `apply_boolean_mask_on()` when a stream exists.

### Projection

`CuDFProjectionExec` looks up the segment stream and clones each projection
expression with that stream. The projected output remains a GPU-backed
`RecordBatch`.

### Aggregate

`CuDFAggregateExec` passes the stream into `CuDFAggregateStream`.

`CuDFAggregateStream` uses the stream for:

- evaluating aggregate argument expressions
- uploading literal aggregate arguments when needed
- concatenating pending cuDF batches into a chunk
- `CuDFGroupBy::aggregate_on()`
- merging chunk state into running state
- casting emitted partial state columns

Chunking is per aggregate stream. With `target_partitions = 8` and the default
`256 MiB` chunk target, each active GPU partition can accumulate up to roughly
that target before flushing, then allocate concat/filter/projection/groupby
intermediates. This is a major reason GPU partition count is also a memory and
allocator-pressure knob.

### Sort

`CuDFSortExec` uses the stream for:

- concatenating input batches
- full-table sort
- top-k sorted-order generation
- gather of top-k rows

### Unload

`CuDFUnloadExec` terminates a GPU segment. It looks up the segment stream,
exports the cuDF table back to host Arrow with `to_arrow_host_on()`, and removes
the stream entry from `CuDFTaskContext` when the stream finishes.

## Dataflow Example

For a simple stream-eligible pipeline:

```text
ParquetExec
  -> CuDFLoadExec(segment=1)
  -> CuDFFilterExec(segment=1)
  -> CuDFProjectionExec(segment=1)
  -> CuDFAggregateExec(segment=1)
  -> CuDFUnloadExec(segment=1)
```

With streams disabled:

```text
target_partitions for GPU subplan = 1
CUDA stream = default stream
CuDFTaskContext stream map is unused
```

With streams enabled and `target_partitions = 8`:

```text
execute(partition=0) uses stream key (1, 0)
execute(partition=1) uses stream key (1, 1)
...
execute(partition=7) uses stream key (1, 7)
```

Each partition is ordered internally because all operators in that partition
use the same stream. Different partitions may enqueue independent work on
different streams.

## Correctness Invariants

The implementation depends on these invariants:

1. A nonzero `segment_id` is assigned only to a component where every cuDF node
   is stream-aware.
2. The same `(segment_id, partition)` stream is used for all dependent GPU work
   inside one segment.
3. Host-to-device pinned input stays alive until the H2D copy on that stream has
   completed.
4. GPU-backed owners must outlive any stream-ordered work that consumes their
   views.
5. `CuDFUnloadExec` is responsible for releasing query stream-map entries at the
   end of a segment.

The fourth invariant is the one to be most skeptical about when debugging
stream regressions. Stream APIs make enqueueing asynchronous; if an owner is
dropped before the stream reaches a consuming operation, the bug can surface as
late CUDA memory errors. The aggregate `flush_pending()` path is the highest
risk area because it rapidly concatenates, clears pending batches, aggregates,
and merges intermediate state.

## Performance Implications

Streams only help when overlap beats the extra coordination cost. In this
implementation, enabling streams also preserves GPU partitioning, so the
following costs grow with partition count:

- more Tokio tasks concurrently entering cuDF
- more CUDA API calls in flight
- more event records and stream synchronization points
- more simultaneous RMM allocations
- more per-partition aggregate chunk buffers
- more intermediate cuDF tables and columns alive at once

For TPC-H q1 on the tested T4, the extra concurrency did not reduce H2D or
kernel device time enough to pay for those costs. The profile details are in
`docs/cuda_streams_results_tpch_1.md`.

## Current Gaps

The current branch proves functional stream plumbing, but it should not yet
treat `target_partitions` as unlimited GPU concurrency.

Open work:

1. Add a query-scoped GPU concurrency limiter so scan parallelism can stay high
   without letting every DataFusion partition enqueue cuDF work at once.
2. Make aggregate chunk memory budget aware of active GPU concurrency. The
   default `256 MiB` target is reasonable for one partition but too large when
   multiplied by many active streams on a 15 GiB GPU.
3. Audit stream-ordered lifetimes in aggregate concat/merge paths.
4. Keep hash join on the default stream until the join implementation has the
   same explicit stream coverage and lifetime audit as the allowlisted nodes.

## Appendix A: Correctness Commands

Run all TPC-H correctness tests with streams enabled and one GPU partition:

```bash
CUDF_TEST_CUDA_STREAMS=1 CUDF_TEST_TARGET_PARTITIONS=1 \
  cargo test -p libcudf-datafusion --features tpch --test tpch_correctness -- --test-threads=1
```

Run all TPC-H correctness tests with streams enabled and eight GPU partitions:

```bash
CUDF_TEST_CUDA_STREAMS=1 CUDF_TEST_TARGET_PARTITIONS=8 \
  cargo test -p libcudf-datafusion --features tpch --test tpch_correctness -- --test-threads=1
```

Run one query while iterating:

```bash
CUDF_TEST_CUDA_STREAMS=1 CUDF_TEST_TARGET_PARTITIONS=8 \
  cargo test -p libcudf-datafusion --features tpch --test tpch_correctness test_tpch_1 -- --test-threads=1
```

## Appendix B: Benchmark Commands

Build the benchmark binary:

```bash
cargo build --release -p libcudf-datafusion-benchmarks --bin dfbench
```

Run TPC-H q1 with streams disabled:

```bash
CUDF_BENCH_CUDA_STREAMS=0 ./target/release/dfbench run \
  --gpu --dataset tpch_sf10 -q q1 -n 8 -i 3 --warmup --no-compare
```

Run TPC-H q1 with streams enabled:

```bash
CUDF_BENCH_CUDA_STREAMS=1 ./target/release/dfbench run \
  --gpu --dataset tpch_sf10 -q q1 -n 8 -i 3 --warmup --no-compare
```

Run TPC-H q1 with streams enabled and a smaller aggregate chunk target:

```bash
CUDF_BENCH_CUDA_STREAMS=1 ./target/release/dfbench run \
  --gpu --dataset tpch_sf10 -q q1 -n 16 -i 3 --warmup \
  --cudf-aggregate-chunk-target-bytes 67108864 \
  --no-compare
```

## Appendix C: Profiling Commands

The detailed TPC-H q1 profiling commands and the sqlite summary extraction
helper are in `docs/cuda_streams_results_tpch_1.md`. The basic nsys capture
shape is:

```bash
CUDF_BENCH_CUDA_STREAMS=1 nsys profile \
  --force-overwrite=true \
  --trace=cuda,nvtx,osrt \
  --stats=true \
  -o /tmp/libcudf-q1-profiles/q1_p8_streams_on \
  ./target/release/dfbench run \
    --gpu --dataset tpch_sf10 -q q1 -n 8 -i 1 \
    --result-dir /tmp/libcudf-q1-profiles/q1_p8_streams_on_result \
    --no-compare
```
