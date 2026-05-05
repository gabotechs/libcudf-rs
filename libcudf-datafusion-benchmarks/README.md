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

Common options are forwarded to both CPU and GPU runs:

```bash
target/release/dfbench harness \
  --dataset tpch_sf1 \
  --query q1,q6,q22 \
  --iterations 5 \
  --partitions 4 \
  --batch-size 8192 \
  --warmup
```

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
