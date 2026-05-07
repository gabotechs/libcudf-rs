//! # TPC-H Pipeline Benchmark
//!
//! ## What this benchmark measures
//!
//! End-to-end wall-clock latency for a TPC-H Q5-style pipeline:
//! Parquet scan -> hash join -> aggregate. Both CPU and GPU paths read the same
//! files from disk; the GPU path uploads the raw input once over PCIe, runs
//! the join and aggregate entirely on-device, then downloads only the 25-row
//! result.
//!
//! This is an integration benchmark, not a microbenchmark. It measures query
//! latency as a user would observe it. It does not isolate individual operators;
//! use `nsys` or `ncu` for per-kernel attribution.
//!
//! ## Per-operator GPU profiling
//!
//! To attribute time between `CuDFLoadExec`, `CuDFHashJoinExec`, and
//! `CuDFAggregateExec`, profile the benchmark binary directly:
//!
//! ```sh
//! cargo build --release --bench tpch_pipeline
//! nsys profile --stats=true \
//!     ./target/release/deps/tpch_pipeline-* \
//!     --bench --profile-time 5
//! nsys stats last_report.nsys-rep
//! ```
//!
//! ## Data
//!
//! TPC-H Parquet files from `data/tpch/correctness_sf{scale}/`.
//! The benchmark defaults to SF=1. Set `LIBCUDF_TPCH_SCALE_FACTOR` to run
//! another generated scale factor:
//!
//! ```sh
//! LIBCUDF_TPCH_SCALE_FACTOR=10 cargo bench -p libcudf-datafusion --bench tpch_pipeline
//! ```
//!
//! Row counts below are for SF=1.
//!
//! | Table    | Rows    | Role               |
//! |----------|---------|--------------------|
//! | orders   | ~1.5 M  | probe (fact) side  |
//! | customer | ~150 K  | build (dim) side   |
//! | result   | 25 rows | one per nation     |

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_physical_plan::{execute_stream, ExecutionPlan};
use futures_util::TryStreamExt;
use libcudf_datafusion::aggregate::count;
use libcudf_datafusion::SessionStateBuilderExt;
use std::env;
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::runtime::Runtime;

const TPCH_SCALE_FACTOR_ENV: &str = "LIBCUDF_TPCH_SCALE_FACTOR";

fn tpch_scale_factor() -> String {
    let raw = env::var(TPCH_SCALE_FACTOR_ENV).unwrap_or_else(|_| "1".to_string());
    let scale_factor = raw.trim().parse::<f64>().unwrap_or_else(|err| {
        panic!("invalid {TPCH_SCALE_FACTOR_ENV} value {raw:?}: {err}");
    });

    if !scale_factor.is_finite() || scale_factor <= 0.0 {
        panic!("{TPCH_SCALE_FACTOR_ENV} must be a positive finite number");
    }

    if scale_factor.fract() == 0.0 {
        format!("{scale_factor:.0}")
    } else {
        scale_factor.to_string()
    }
}

fn tpch_data_dir(scale_factor: &str) -> PathBuf {
    let data_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(format!("data/tpch/correctness_sf{scale_factor}"));
    if !data_dir.exists() {
        panic!(
            "TPC-H SF={scale_factor} data not found at {}",
            data_dir.display()
        );
    }
    data_dir
}

const PIPELINE_SQL: &str = "
    SELECT c.c_nationkey, COUNT(c.c_custkey) AS cnt
    FROM orders o
    JOIN customer c ON o.o_custkey = c.c_custkey
    GROUP BY c.c_nationkey
";

async fn cpu_ctx(base: PathBuf) -> SessionContext {
    let ctx = SessionContext::new();
    ctx.register_parquet(
        "orders",
        base.join("orders").to_str().unwrap(),
        ParquetReadOptions::new(),
    )
    .await
    .unwrap();
    ctx.register_parquet(
        "customer",
        base.join("customer").to_str().unwrap(),
        ParquetReadOptions::new(),
    )
    .await
    .unwrap();
    ctx
}

async fn gpu_ctx(base: PathBuf) -> SessionContext {
    // Single partition: DataFusion plans AggregateMode::Single instead of
    // Partial -> RepartitionExec -> Final. The two-phase path sends partial-state
    // batches through a CPU RepartitionExec, which strips CuDFColumnView wrappers.
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(SessionConfig::new().with_target_partitions(1))
        .with_cudf_planner()
        .build();
    let ctx = SessionContext::from(state);
    ctx.register_udaf((*count()).clone());
    ctx.register_parquet(
        "orders",
        base.join("orders").to_str().unwrap(),
        ParquetReadOptions::new(),
    )
    .await
    .unwrap();
    ctx.register_parquet(
        "customer",
        base.join("customer").to_str().unwrap(),
        ParquetReadOptions::new(),
    )
    .await
    .unwrap();
    ctx
}

async fn build_plan(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
    ctx.sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap()
}

fn bench_tpch_pipeline(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let scale_factor = tpch_scale_factor();
    let data_dir = tpch_data_dir(&scale_factor);
    let mut group = c.benchmark_group(format!("tpch_pipeline/orders_x_customer/sf{scale_factor}"));

    let cpu = rt.block_on(cpu_ctx(data_dir.clone()));
    let gpu = rt.block_on(gpu_ctx(data_dir));
    let cpu_task_ctx = cpu.task_ctx();
    let gpu_task_ctx = gpu.task_ctx();

    // Warm the OS page cache so Parquet I/O does not appear in timed iterations.
    rt.block_on(async {
        let plan = build_plan(&cpu, PIPELINE_SQL).await;
        execute_stream(plan, cpu_task_ctx.clone())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let plan = build_plan(&gpu, PIPELINE_SQL).await;
        execute_stream(plan, gpu_task_ctx.clone())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
    });

    group.bench_function("cpu", |b| {
        b.iter_batched(
            || rt.block_on(build_plan(&cpu, PIPELINE_SQL)),
            |plan| {
                rt.block_on(async {
                    black_box(
                        execute_stream(plan, cpu_task_ctx.clone())
                            .unwrap()
                            .try_collect::<Vec<_>>()
                            .await
                            .unwrap(),
                    );
                })
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function("gpu", |b| {
        b.iter_batched(
            || rt.block_on(build_plan(&gpu, PIPELINE_SQL)),
            |plan| {
                rt.block_on(async {
                    black_box(
                        execute_stream(plan, gpu_task_ctx.clone())
                            .unwrap()
                            .try_collect::<Vec<_>>()
                            .await
                            .unwrap(),
                    );
                })
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_tpch_pipeline);
criterion_main!(benches);
