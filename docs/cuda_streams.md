# CUDA Streams

## What a CUDA Stream Is

CUDA streams are ordered queues of GPU work such as kernel launches or host <-> device copies.
CUDA operations submitted to the same stream execute serially. Different streams may
run concurrently if:
- the GPU has available resources; and
- the hardware allows it.

Streams can be used by different host threads. CUDA documents a stream as a sequence of commands
"possibly issued by different host threads" that execute in order. One caveat remains: stream
ordering does not make shared GPU memory access automatically safe. It is not safe to launch work
on different streams if the operations have unsynchronized read/write dependencies on the same GPU
memory.

The "default stream" is the one used by the CUDA runtime when no stream is specified, meaning all operations
are serialized.

Streams are either "sync" or "nonblocking". Sync streams serialize with the default stream. For the
purposes of this document, assume all explicitly created streams are nonblocking.

## Why Streams Help

Streams are useful because they let independent GPU work from different partitions overlap instead
of all serializing on the default stream.

Example: `p0` executes a sort while `p1` executes a load + filter

Without explicit streams, work is serialized (note that p0 and p1 run concurrently, so there are
multiple total orderings possible):

```text
default stream:
  [load p0] -> [sort p1] -> [filter p0]
```

With `p0` on stream 0 and `p1` on stream 1, the GPU may overlap the sort on `p1` with the
load/filter work on `p0`:
```text
time ---->

stream 0: [load p0] -> [filter p0]
stream 1: [sort p1 ---------------]
```

Another useful pattern is copy/compute overlap:

```text
time ---->

stream 0: [H->D copy]
stream 1: [aggregate]
```

This does not guarantee perfect overlap. Heavy sorts, joins, and groupbys may still saturate the
GPU, preventing work on another stream from starting. However, explicit streams allow
overlap when there is room for it. This is better than the default-stream model which always serializes
everything.

## Integration with libcudf-datafusion

### GPU Segments

A "GPU Segment" is a sequence of datafusion operators which all run on the GPU without an expensive
H <-> D copy.

Today, `libcudf-datafusion` can rewrite these operators onto cuDF:

- `CuDFLoadExec`
- `CuDFUnloadExec`
- `CuDFFilterExec`
- `CuDFProjectionExec`
- `CuDFSortExec`
- `CuDFHashJoinExec`
- `CuDFAggregateExec`
- `CuDFCoalesceBatchesExec`

A typical GPU segment looks like so:

```text
CPU source
  -> CuDFLoadExec
  -> filter / projection / sort / join / aggregate
  -> CuDFUnloadExec
  -> CPU operators
```

As this project develops, we will likely try to make pipelines as long as possible
to avoid expensive H <-> D copies.

### Streaming Model in libcudf-datafusion

For a given GPU segment in a query, use a single stream for those operations.

#### Example 1: Single GPU Segment

Consider a query `Q` which executes:

```text
load -> filter -> projection -> aggregate -> unload -> non_gpu_operation
```

This query has `P` partitions in which GPU work can be parallelized. Map each partition to a stream:

```text
  partition 0 -> stream S(Q,0)
  partition 1 -> stream S(Q,1)
  partition 2 -> stream S(Q,2)
  ...
  partition P -> stream S(Q,P)
```

We can create a stream per partition for the entire `load -> filter -> projection -> aggregate -> unload`
segment. The segment for each partition can be executed concurrently.

#### Example 2: Multiple GPU Segments

Today `RepartitionExec` is still CPU-only, so a query may contain multiple distinct GPU segments
separated by CPU work. For example:

```text
load -> filter -> projection -> aggregate -> unload
-> repartition ->
load -> aggregate -> unload
```

This should be treated as two GPU segments, each with its own stream:

```text
segment 1:
  load -> filter -> projection -> aggregate -> unload

CPU boundary:
  repartition

segment 2:
  load -> aggregate -> unload
```

On entering a segment, create or acquire a stream for that segment. On leaving the segment,
release it.

### Stream Lifecycle

Streams should be created lazily when a partition first enters a GPU segment and released when that
GPU segment finishes.

In practice:

- `CuDFLoadExec` is the natural place to first get or create the stream
- downstream GPU operators should reuse that same stream
- `CuDFUnloadExec` is usually the last consumer in the segment
- stream ownership should still live in a query/partition execution context, not in `Load` or `Unload` themselves

For a query like:

```text
load -> filter -> projection -> aggregate -> unload -> repartition -> load -> aggregate -> unload
```

the lifecycle is:

```text
segment 1:
  load -> filter -> projection -> aggregate -> unload
  uses stream S1

CPU repartition

segment 2:
  load -> aggregate -> unload
  uses stream S2
```

### Alternatives Considered

#### Create a fresh stream inside `CuDFAggregateExec::execute()`

Rejected because it is too narrow. A partition usually goes through multiple GPU operators, and
they should share one stream.

For example, this:

```text
load on stream A
aggregate on stream B
unload on stream C
```

is worse than this:

```text
load on stream S
aggregate on stream S
unload on stream S
```

#### Let `CuDFLoadExec` own creation and `CuDFUnloadExec` own destruction directly

Almost right, but still not ideal. They are the natural first and last users of a stream, but
ownership should stay in a query/partition execution context so that:

- stream state is managed in one place
- the same model works for all GPU operators
- repartition boundaries are handled as separate GPU segments rather than implicit stream continuity

## Risks / Future Work

### Hardware Contention

We may lose parallelism or encounter errors due to resource limitations:

- failed allocation (error case)
- bandwidth contention (lost parallelism)
- SM saturation (lost parallelism)
- copy engine contention (lost parallelism)

Ideally, we could track queue sizes, available memory, SM utilization, etc. to try to schedule
work more intelligently on the GPU.

For now, it's acceptable to defer all scheduling and queuing to the CUDA library.

## References:

- CUDA streams:
  https://docs.nvidia.com/cuda/archive/9.1/cuda-c-programming-guide/index.html#streams
- CUDA concurrent execution:
  https://docs.nvidia.com/cuda/archive/9.1/cuda-c-programming-guide/index.html#concurrent-execution-host-device
- CUDA overlapping behavior:
  https://docs.nvidia.com/cuda/archive/9.1/cuda-c-programming-guide/index.html#overlapping-behavior
- CUDA default stream:
  https://docs.nvidia.com/cuda/archive/9.1/cuda-c-programming-guide/index.html#default-stream
- RMM stream flags:
  https://docs.rapids.ai/api/rmm/stable/cpp/cuda_streams/#_CPPv4N3rmm11cuda_stream5flags
- PyTorch CUDA stream wrapper example:
  https://fossies.org/linux/pytorch/c10/cuda/CUDAStream.h

