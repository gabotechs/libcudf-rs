# libcudf-datafusion benchmarks

This crate contains the `dfbench` command for preparing datasets, running
benchmarks, comparing branch-local benchmark results, and producing paired
CPU/GPU benchmark reports.

## Build

Build the benchmark binary in release mode before collecting timings:

```bash
cargo build -p libcudf-datafusion-benchmarks --release
```

The binary is written to:

```text
target/release/dfbench
```

## Datasets

Datasets live under:

```text
libcudf-datafusion-benchmarks/data/<dataset>/
```

The `data/` directory is ignored by git. Use the `prepare-*` subcommands to
create benchmark datasets:

```bash
target/release/dfbench prepare-tpch --help
target/release/dfbench prepare-tpcds --help
target/release/dfbench prepare-clickbench --help
```

Dataset names may include scale or variant details, for example `tpch_sf1`.
The query family is inferred from the dataset prefix before the first `_`.

## Branch Comparison Runs

Use `dfbench run` when you want the normal branch-local benchmark flow:

```bash
target/release/dfbench run --dataset tpch_sf1 --query q22 --iterations 3
target/release/dfbench run --dataset tpch_sf1 --query q22 --iterations 3 --gpu
```

This path writes comparison state under the dataset:

```text
data/<dataset>/previous.json
data/<dataset>/.results/<branch>/<dataset> <query>.json
```

`dfbench run` compares the new run against `previous.json` when one is present,
then stores the new results for the next run.

## Paired CPU/GPU Harness

Use `dfbench harness` when you want a reproducible report with CPU and GPU runs
using the same options:

```bash
target/release/dfbench harness --dataset tpch_sf1 --iterations 3 --partitions 4
```

The harness does not compare query outputs. Correctness belongs in tests. It
also does not write to `data/<dataset>/previous.json` or
`data/<dataset>/.results/<branch>/`; those paths are reserved for
`dfbench run`.

By default, harness artifacts are written to:

```text
benchmark-results/<dataset>/<run-id>/
```

For example:

```text
benchmark-results/tpch_sf1/1760000000_abcd123/
  report.md
  metadata.json
  logs/cpu.log
  logs/gpu.log
  cpu/q1.json
  gpu/q1.json
```

Override the artifact root or run id with:

```bash
target/release/dfbench harness \
  --dataset tpch_sf1 \
  --output /tmp/bench-results \
  --run-id before-change
```

That writes to:

```text
/tmp/bench-results/tpch_sf1/before-change/
```

## Harness Options

Common options are forwarded to both CPU and GPU runs, except for execution
batch size, which can be configured independently:

```bash
target/release/dfbench harness \
  --dataset tpch_sf1 \
  --query q1,q6,q22 \
  --iterations 5 \
  --partitions 4 \
  --batch-size 8192 \
  --warmup
```

`--batch-size` sets DataFusion's `execution.batch_size`. The harness passes
this value to the CPU run. GPU runs use `65536` by default; pass
`--gpu-execution-batch-size` to override that value for the target workload and
GPU.

`--cudf-device-pool-max-bytes` sets the RMM device-memory pool cap for GPU
runs. cuDF aggregate chunks derive their default target from that cap:
`clamp(pool_max / 16, 64 MiB, 1 GiB)`. Use
`--cudf-aggregate-chunk-target-bytes` only when testing an explicit aggregate
chunk budget.

Capture physical plans for selected queries:

```bash
target/release/dfbench harness \
  --dataset tpch_sf1 \
  --query q22 \
  --plan-query q22
```

Plan logs are written under:

```text
benchmark-results/<dataset>/<run-id>/plans/
```

Capture an Nsight Systems profile for selected GPU queries:

```bash
target/release/dfbench harness \
  --dataset tpch_sf1 \
  --query q22 \
  --profile-query q22
```

Profiles require `nsys` on `PATH` and are written under:

```text
benchmark-results/<dataset>/<run-id>/profiles/
```

## Profile Comparison

Use `dfbench profile-compare` to compare Nsight SQLite artifacts from two
profiled harness runs:

```bash
target/release/dfbench profile-compare \
  --dataset tpch_sf1 \
  --baseline-run-id before-change \
  --candidate-run-id after-change
```

Limit the comparison to selected queries with:

```bash
target/release/dfbench profile-compare \
  --dataset tpch_sf1 \
  --baseline-run-id before-change \
  --candidate-run-id after-change \
  --query q6,q12,q19
```

Profile comparisons are written under:

```text
benchmark-results/<dataset>/comparisons/<baseline-run-id>__<candidate-run-id>/
  profile-compare.md
  profile-compare.json
```

The report compares CUDA runtime API time, device allocation/free calls,
host allocation/free time, memcpy activity, synchronization API time, kernel
launch calls, and candidate-run runtime/kernel hotspots. This path reads
existing profile artifacts; it does not run benchmarks.

The generated `profile-compare.md` has these sections:

- `Total Profile Cost`: aggregate CUDA runtime, copy, synchronization, launch,
  and kernel activity costs across the compared profiles.
- `Memcpy bytes`: total bytes moved by recorded CUDA memcpy activity.
- `Per Query Deltas`: compact per-query candidate-minus-baseline changes for
  the categories that usually explain benchmark movement.
- `Per Query Runtime API Changes`: the largest CUDA runtime API deltas by
  absolute time for each query. This is usually the quickest way to understand
  why a profile got faster or slower.
- `Candidate Hotspots`: candidate-only top costs after the change. Use this to
  decide what remains expensive after the comparison.

The profile terms have specific meanings:

- `Runtime API`: CPU-side time spent inside CUDA runtime calls, such as
  `cudaMemcpyAsync`, `cudaLaunchKernel`, `cudaStreamSynchronize`, and
  allocation calls. High runtime API time often means launch, synchronization,
  copy setup, or allocator overhead rather than GPU compute time.
- `Device alloc/free API`: runtime API calls that allocate or free device
  memory, such as `cudaMalloc` and `cudaFree`.
- `Host alloc/free API`: runtime API calls that allocate or free host-side
  pinned memory, such as `cudaHostAlloc`, `cudaMallocHost`, and
  `cudaFreeHost`.
- `Memcpy API`: CPU-side time spent issuing CUDA memcpy calls. This is not the
  same as transfer-engine activity.
- `Memcpy activity`: CUPTI-recorded copy activity, including count, elapsed
  copy time, bytes, and direction such as host-to-device or device-to-host.
- `Sync API`: CUDA synchronization calls that can force the CPU to wait for GPU
  work.
- `Kernel launch API`: CPU-side time spent launching GPU kernels.
- `Kernel activity`: GPU-side kernel execution time recorded by CUPTI.
- `Kernel groups`: candidate kernel activity grouped by Nsight short kernel
  name. `unique full names` shows how many distinct full demangled symbols were
  collapsed into that short group.

## Reading Reports

The generated `report.md` includes:

- git commit, branch, dirty-worktree state, CPU core count, and GPU metadata
- exact CPU and GPU commands
- paths to per-query CPU/GPU JSON artifacts
- average, median, min, and max timing per query
- row counts and per-query status
- total average speedup and geometric mean speedup

Queries with failed or partial iterations are marked in the status column and
are excluded from speedup totals.

## Notes

- Run release builds for timing data.
- Keep correctness checks in tests, not in the benchmark harness.
- `benchmark-results/` is ignored by git.
- The harness keeps CPU and GPU runs in separate subprocesses so command logs
  and process-level setup are reproducible.
