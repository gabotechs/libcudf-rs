# TPC-H q1 streams results: p=1 vs p=8

Date: 2026-05-08

Dataset: `tpch_sf10`

Query: TPC-H `q1`

Binary: `target/release/dfbench`

GPU: Tesla T4, 15 GiB

Aggregate chunk target: default `256 MiB`

## Summary

For q1, streams are fine at `p=1` and pathological at higher GPU partition
counts. Replacing the failed `p=16` case with `p=8` avoids OOM for a single
iteration, but it is still much slower than the default-stream plan.

The profiles show that `p=8 streams=on` is not slower because the useful GPU
kernels or H2D copies take much longer. Device H2D time and bytes are almost
unchanged. Kernel device time rises only modestly. The slowdown is mostly
host-side CUDA API submission/synchronization and cuDF/RMM range time around
`from_arrow_host`, `apply_boolean_mask`/`copy_if`, and allocation-heavy paths.
In other words, extra streams created more concurrent GPU-side pressure, but
they did not expose enough useful overlap to reduce the total device work.

This matches the benchmark sweep:

- `p=1 streams=on` is close to `p=1 streams=off`.
- `p=2`, `p=4`, and `p=8 streams=on` get progressively slower.
- `p=16 streams=on` with the default `256 MiB` aggregate chunk target becomes
  extremely slow and then OOMs.
- Lowering the chunk target to `64 MiB` avoids the `p=16` OOM, but it remains
  slower than the single-GPU-partition plan.

## Benchmark Results

These are non-profiled wall-clock runs with one warmup and three timed
iterations:

| case | iterations (ms) | avg/result |
|---|---:|---:|
| `p=1 streams=off` | 5622, 5607, 5612 | 5613 ms |
| `p=1 streams=on` | 5487, 5556, 5588 | 5542 ms |
| `p=16 streams=off` | 5496, 5466, 5460 | 5473 ms |
| `p=16 streams=on`, default chunk | 40048, 53693, OOM | failed |
| `p=16 streams=on`, `64 MiB` chunk | 8667, 10529, 12716 | 10637 ms |
| `p=2 streams=on`, default chunk | 7697, 8075, 8435 | 8068 ms |
| `p=4 streams=on`, default chunk | 10298, 11713, 13486 | 11832 ms |
| `p=8 streams=on`, default chunk | 16431, 23158, 31169 | 23585 ms |

Raw benchmark logs:

```text
/tmp/libcudf-q1-bench-results/
/tmp/libcudf-q1-followup-results/
```

## Profile Method

The profiles below are one query execution per process, without `--warmup`, to
keep each trace focused on one q1 execution. These profiled wall times are
therefore higher than the warmed benchmark times, but the relative attribution
is still useful.

Raw profile artifacts:

```text
/tmp/libcudf-q1-profiles/q1_p1_streams_off.{nsys-rep,sqlite,stdout,stderr}
/tmp/libcudf-q1-profiles/q1_p1_streams_on.{nsys-rep,sqlite,stdout,stderr}
/tmp/libcudf-q1-profiles/q1_p8_streams_off.{nsys-rep,sqlite,stdout,stderr}
/tmp/libcudf-q1-profiles/q1_p8_streams_on.{nsys-rep,sqlite,stdout,stderr}
```

## Profile Wall Time

| case | profiled wall time | CUDA activity span | active CUDA streams |
|---|---:|---:|---:|
| `p=1 streams=off` | 9948 ms | 8913 ms | 1 |
| `p=1 streams=on` | 9758 ms | 8716 ms | 2 |
| `p=8 streams=off` | 9956 ms | 8936 ms | 1 |
| `p=8 streams=on` | 17161 ms | 16136 ms | 11 |

`p=8 streams=off` still has one active CUDA stream because
`with_cudf_planner()` collapses the GPU subplan to one partition when streams
are disabled. The `-n 8` still matters for the scan leaves, but not for GPU
operator concurrency.

`p=8 streams=on` really does use multiple CUDA streams, but the extra streams
increase the active CUDA span by about 7.2 s.

## CUDA Device Work

| case | kernel device time | kernel launches | memcpy device time | memcpy bytes |
|---|---:|---:|---:|---:|
| `p=1 streams=off` | 767 ms | 118804 | 1089 ms | 4.80 GiB |
| `p=1 streams=on` | 772 ms | 118804 | 1093 ms | 4.80 GiB |
| `p=8 streams=off` | 797 ms | 118804 | 1090 ms | 4.80 GiB |
| `p=8 streams=on` | 911 ms | 119008 | 1099 ms | 4.80 GiB |

This is the important profile result: `p=8 streams=on` is 7.2 s slower in
profiled wall time, but useful device work barely changes:

- H2D bytes are the same.
- memcpy device time is essentially the same.
- kernel device time increases by only about 114 ms vs `p=8 streams=off`.

So the slowdown is not explained by "more GPU compute" or "slower H2D copies".

## Why More Parallelism Does Not Save Time Here

TPC-H q1 is not behaving like a small number of long independent kernels where
extra streams can simply fill idle GPU time. The plan is a large number of
short cuDF operations around load, filter, projection, and aggregate chunk
management. The single-GPU-partition/default-stream path already keeps the
device busy enough that the remaining time is mostly host submission,
synchronization, allocator behavior, and fixed data movement.

The p=8 streams-on profile confirms that more CUDA streams were actually used:
`p=8 streams=off` used one CUDA stream, while `p=8 streams=on` used eleven.
If stream parallelism were helping this query, we would expect the CUDA
activity span, H2D time, or kernel critical path to shrink. They did not:

- H2D volume is fixed at about `4.80 GiB`, so streams cannot reduce the amount
  of data copied.
- H2D device time is effectively unchanged: `1090 ms` off vs `1099 ms` on.
- Kernel device time only grows from `797 ms` to `911 ms`, which is far smaller
  than the `~7.2 s` wall-time regression.
- CUDA activity span grows from `8936 ms` to `16136 ms`.
- CUDA API time grows substantially: `cudaMemcpyAsync`, `cudaEventRecord`, and
  `cudaLaunchKernel` all become much more expensive in aggregate.
- NVTX range time balloons in allocation-heavy and filter/load paths, especially
  `from_arrow_host`, `apply_boolean_mask`/`copy_if`, and `allocate_like`.

So the added streams are not uncovering hidden GPU throughput. They are mostly
letting more DataFusion partitions enter cuDF at the same time. The trace shows
that the result is longer queueing and coordination, not shorter device work.
The number of in-flight uploads, events, launches, allocations, masks, concat
inputs, and aggregate intermediates grows. The extra work queues on the GPU and
RMM allocator, and the host threads spend more time submitting and waiting.

The implementation detail that matters most is aggregate chunking. The default
chunk target is `256 MiB` per `CuDFAggregateStream`, not per query. With
`p=8 streams=on`, eight partitions can each hold pending GPU batches and then
allocate concat/filter/projection/groupby intermediates. That multiplies the
memory footprint and allocator pressure. With `p=16`, the same effect becomes
large enough to hit OOM. Lowering the chunk target to `64 MiB` avoids the p=16
OOM, which supports the memory-pressure explanation, but it still does not make
the high-partition stream plan faster than the one-partition GPU plan.

This is also why `p=8 streams=off` is close to `p=1 streams=off`: when streams
are disabled, the planner still lets the leaf scan side use the requested
partition count, but it collapses the GPU subplan to one partition. That keeps
CPU-side scan parallelism without multiplying concurrent cuDF work.

The practical conclusion is that q1 needs controlled GPU concurrency, not a
direct mapping from DataFusion `target_partitions` to active CUDA streams.

## CUDA Runtime API Time

Top CUDA API totals:

| API | p=8 off | p=8 on | delta |
|---|---:|---:|---:|
| `cudaMemcpyAsync` | 2326 ms | 3726 ms | +1400 ms |
| `cudaEventRecord` | 1194 ms | 2315 ms | +1121 ms |
| `cudaStreamSynchronize` | 1784 ms | 1962 ms | +178 ms |
| `cudaLaunchKernel` | 924 ms | 1816 ms | +892 ms |
| `cudaStreamWaitEvent` | ~0 ms | 91 ms | +91 ms |

The API side roughly doubles for launches/events and grows substantially for
memcpy submission. That is consistent with many DataFusion tasks feeding the
GPU at once, not with faster GPU overlap.

## NVTX Range Time

NVTX ranges are summed across host threads and streams, so totals can exceed
wall time. The ballooning still tells us where host-side waits and queued GPU
work concentrate.

| NVTX range | p=8 off | p=8 on |
|---|---:|---:|
| `apply_boolean_mask` | 3868 ms | 64215 ms |
| `copy_if` | 3859 ms | 64201 ms |
| `from_arrow_host` | 2099 ms | 29458 ms |
| `allocate_like` | 730 ms | 25944 ms |
| `make_fixed_width_column` | 977 ms | 22260 ms |
| `make_fixed_point_column` | 608 ms | 12935 ms |
| `make_numeric_column` | 375 ms | 11502 ms |
| `thrust::exclusive_scan` | 606 ms | 10299 ms |
| `binary_operation` | 1090 ms | 10212 ms |
| `aggregate` | 380 ms | 1536 ms |
| `concatenate` | not top | 1225 ms |

The hot ranges are load/filter/projection/allocation paths, not just the
hash-aggregate kernel. `aggregate` does grow, but it is not the dominant
ballooning range in the trace.

## Interpretation

The streams-on plan changes two things at once:

1. It enables non-default CUDA streams.
2. It preserves GPU partitioning, so q1 runs multiple independent GPU
   partitions concurrently.

The second effect is the expensive one. With the default `256 MiB` aggregate
chunk target, every active partition can hold a large pending GPU batch set and
then allocate concat/projection/filter/groupby intermediates. At p=8 this is
already enough to substantially increase API overhead and allocator/range wait
time. At p=16 it eventually exhausts the RMM pool.

The `64 MiB` chunk experiment supports this: it avoids p=16 OOM and cuts the
runtime from 40-53 s to 8-13 s. That does not make p=16 fast, but it confirms
the default chunk target is too large when multiplied by many concurrent GPU
partitions.

The `32 MiB` crash is a separate warning sign. Smaller chunks increase
`flush_pending` frequency, which increases async concat/merge/lifetime churn.
The `cudaErrorMisalignedAddress` suggests there may be an async lifetime bug
around stream-ordered work and dropped GPU-backed batches/intermediates. The
most suspicious area is `CuDFAggregateStream::flush_pending`, which enqueues
`concat_on`/aggregate work on a stream and then quickly clears or replaces
source/intermediate owners.

## Recommended Fix Direction

1. Do not let `target_partitions` directly become GPU stream concurrency for
   q1-sized inputs. Add a query-scoped GPU concurrency limit in
   `CuDFTaskContext` or the execution nodes. Keep scan leaf parallelism high,
   but cap active GPU partitions based on a memory budget.

2. Make aggregate chunk target effectively per-query, not per-partition:
   use something like `total_budget / active_gpu_partitions`, with a floor.
   The default `256 MiB` is reasonable for one partition but too large for
   8-16 concurrent streams on a 15 GiB T4.

3. Audit stream-ordered lifetimes in `CuDFAggregateStream::flush_pending` and
   merge paths. Either keep source GPU batches/intermediates alive until the
   stream has reached the consuming work, or add explicit stream synchronization
   at narrow ownership boundaries. The 32 MiB `cudaErrorMisalignedAddress`
   should be treated as a correctness bug, not a performance artifact.

4. After capping concurrency/lifetimes, rerun the warmed `p=8 streams=on`
   benchmark and profile it against `p=8 streams=off`. Appendix A and
   Appendix B have the exact command shapes.

## Appendix A: Benchmark Reproduction Commands

Build the benchmark binary:

```bash
cargo build --release -p libcudf-datafusion-benchmarks --bin dfbench
```

Run the warmed benchmark cases:

```bash
mkdir -p /tmp/libcudf-q1-bench-results

CUDF_BENCH_CUDA_STREAMS=0 ./target/release/dfbench run \
  --gpu --dataset tpch_sf10 -q q1 -n 1 -i 3 --warmup \
  --result-dir /tmp/libcudf-q1-bench-results/p1_streams_0 \
  --no-compare 2>&1 | tee /tmp/libcudf-q1-bench-results/p1_streams_0.log

CUDF_BENCH_CUDA_STREAMS=1 ./target/release/dfbench run \
  --gpu --dataset tpch_sf10 -q q1 -n 1 -i 3 --warmup \
  --result-dir /tmp/libcudf-q1-bench-results/p1_streams_1 \
  --no-compare 2>&1 | tee /tmp/libcudf-q1-bench-results/p1_streams_1.log

CUDF_BENCH_CUDA_STREAMS=0 ./target/release/dfbench run \
  --gpu --dataset tpch_sf10 -q q1 -n 16 -i 3 --warmup \
  --result-dir /tmp/libcudf-q1-bench-results/p16_streams_0 \
  --no-compare 2>&1 | tee /tmp/libcudf-q1-bench-results/p16_streams_0.log

CUDF_BENCH_CUDA_STREAMS=1 ./target/release/dfbench run \
  --gpu --dataset tpch_sf10 -q q1 -n 16 -i 3 --warmup \
  --result-dir /tmp/libcudf-q1-bench-results/p16_streams_1 \
  --no-compare 2>&1 | tee /tmp/libcudf-q1-bench-results/p16_streams_1.log
```

The p=16 streams-on case OOMed with the default aggregate chunk target. The
follow-up sweep used lower stream counts and a smaller p=16 chunk:

```bash
mkdir -p /tmp/libcudf-q1-followup-results

for p in 2 4 8; do
  CUDF_BENCH_CUDA_STREAMS=1 ./target/release/dfbench run \
    --gpu --dataset tpch_sf10 -q q1 -n "$p" -i 3 --warmup \
    --result-dir "/tmp/libcudf-q1-followup-results/p${p}_streams_1_default" \
    --no-compare 2>&1 | tee "/tmp/libcudf-q1-followup-results/p${p}_streams_1_default.log"
done

CUDF_BENCH_CUDA_STREAMS=1 ./target/release/dfbench run \
  --gpu --dataset tpch_sf10 -q q1 -n 16 -i 3 --warmup \
  --cudf-aggregate-chunk-target-bytes 67108864 \
  --result-dir /tmp/libcudf-q1-followup-results/p16_streams_1_chunk64m \
  --no-compare 2>&1 | tee /tmp/libcudf-q1-followup-results/p16_streams_1_chunk64m.log
```

After a concurrency or lifetime fix, rerun the focused warmed case:

```bash
CUDF_BENCH_CUDA_STREAMS=1 ./target/release/dfbench run \
  --gpu --dataset tpch_sf10 -q q1 -n 8 -i 3 --warmup --no-compare
```

## Appendix B: Profile Capture Commands

Run the four nsys profiles:

```bash
mkdir -p /tmp/libcudf-q1-profiles

for p in 1 8; do
  for streams in 0 1; do
    label="off"
    if [ "$streams" = "1" ]; then label="on"; fi

    CUDF_BENCH_CUDA_STREAMS="$streams" nsys profile \
      --force-overwrite=true \
      --trace=cuda,nvtx,osrt \
      --stats=true \
      -o "/tmp/libcudf-q1-profiles/q1_p${p}_streams_${label}" \
      ./target/release/dfbench run \
        --gpu --dataset tpch_sf10 -q q1 -n "$p" -i 1 \
        --result-dir "/tmp/libcudf-q1-profiles/q1_p${p}_streams_${label}_result" \
        --no-compare \
      > "/tmp/libcudf-q1-profiles/q1_p${p}_streams_${label}.stdout" \
      2> "/tmp/libcudf-q1-profiles/q1_p${p}_streams_${label}.stderr"
  done
done
```

Generate the same built-in nsys summaries shown in the captured stdout:

```bash
nsys stats \
  -r nvtx_sum,cuda_api_sum,cuda_gpu_kern_sum,cuda_gpu_mem_time_sum,cuda_gpu_mem_size_sum \
  /tmp/libcudf-q1-profiles/q1_p8_streams_on.nsys-rep
```

## Appendix C: Profile Summary Extraction

Use this Python helper to extract the compact comparison tables from the
exported `.sqlite` files. The machine used for this report did not have the
`sqlite3` CLI installed, so this uses Python's standard library:

```bash
python3 - <<'PY'
import pathlib
import re
import sqlite3

root = pathlib.Path("/tmp/libcudf-q1-profiles")
cases = [
    "q1_p1_streams_off",
    "q1_p1_streams_on",
    "q1_p8_streams_off",
    "q1_p8_streams_on",
]

def ms(ns):
    return ns / 1_000_000

for case in cases:
    con = sqlite3.connect(root / f"{case}.sqlite")
    cur = con.cursor()
    wall = None
    out = root / f"{case}.stdout"
    if out.exists():
        m = re.search(r"iteration 0 took ([0-9.]+) ms", out.read_text())
        wall = float(m.group(1)) if m else None

    starts = []
    ends = []
    for table in ("CUPTI_ACTIVITY_KIND_KERNEL", "CUPTI_ACTIVITY_KIND_MEMCPY"):
        row = cur.execute(f"select min(start), max(end) from {table}").fetchone()
        starts.append(row[0])
        ends.append(row[1])
    cuda_span = ms(max(ends) - min(starts))

    kernel_time, kernel_count = cur.execute(
        "select sum(end-start), count(*) from CUPTI_ACTIVITY_KIND_KERNEL"
    ).fetchone()
    h2d_time, h2d_bytes = cur.execute(
        "select sum(end-start), sum(bytes) from CUPTI_ACTIVITY_KIND_MEMCPY where copyKind = 1"
    ).fetchone()
    streams = cur.execute(
        "select count(distinct streamId) from ("
        "select streamId from CUPTI_ACTIVITY_KIND_KERNEL "
        "union all "
        "select streamId from CUPTI_ACTIVITY_KIND_MEMCPY)"
    ).fetchone()[0]

    print(
        case,
        f"wall_ms={wall:.0f}" if wall else "wall_ms=unknown",
        f"cuda_span_ms={cuda_span:.0f}",
        f"streams={streams}",
        f"kernel_ms={ms(kernel_time):.0f}",
        f"kernel_launches={kernel_count}",
        f"h2d_ms={ms(h2d_time):.0f}",
        f"h2d_gib={h2d_bytes / 1024**3:.2f}",
    )

    print("  top CUDA APIs:")
    for name, count, total_ns in cur.execute("""
        select replace(s.value, '_v3020', ''), count(*), sum(r.end-r.start)
        from CUPTI_ACTIVITY_KIND_RUNTIME r
        join StringIds s on s.id = r.nameId
        group by s.value
        order by sum(r.end-r.start) desc
        limit 8
    """):
        print(f"    {name}: {ms(total_ns):.0f} ms ({count} calls)")

    print("  top NVTX ranges:")
    for name, count, total_ns in cur.execute("""
        select coalesce(n.text, s.value), count(*), sum(n.end-n.start)
        from NVTX_EVENTS n
        left join StringIds s on s.id = n.textId
        where n.end is not null
        group by 1
        order by sum(n.end-n.start) desc
        limit 10
    """):
        print(f"    {name}: {ms(total_ns):.0f} ms ({count} ranges)")
PY
```
