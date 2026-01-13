use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::hint::black_box;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_physical_plan::execute_stream;
use futures_util::TryStreamExt;
use libcudf_datafusion::{CuDFConfig, HostToCuDFRule};
use tokio::runtime::Runtime;

const SIZES: [usize; 3] = [10_000, 100_000, 1_000_000];

fn create_test_table(num_rows: usize) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("group_key", DataType::Int32, false),
        Field::new("value", DataType::Float64, false),
    ]);

    // Shuffled IDs for sort benchmarks
    let id_data: Vec<i64> = (0..num_rows as i64)
        .map(|x| (x.wrapping_mul(31337)) % (num_rows as i64))
        .collect();
    // 100 groups for aggregation benchmarks
    let group_data: Vec<i32> = (0..num_rows).map(|x| (x % 100) as i32).collect();
    let value_data: Vec<f64> = (0..num_rows).map(|x| x as f64 * 1.5).collect();

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(id_data)),
            Arc::new(Int32Array::from(group_data)),
            Arc::new(Float64Array::from(value_data)),
        ],
    )
    .unwrap()
}

async fn create_gpu_context(batch: RecordBatch) -> SessionContext {
    let config = SessionConfig::new().with_option_extension(CuDFConfig::default());
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .with_physical_optimizer_rule(Arc::new(HostToCuDFRule))
        .build();
    let ctx = SessionContext::from(state);
    ctx.register_batch("test_table", batch).unwrap();
    ctx
}

async fn create_cpu_context(batch: RecordBatch) -> SessionContext {
    let ctx = SessionContext::new();
    ctx.register_batch("test_table", batch).unwrap();
    ctx
}

async fn execute_query(ctx: &SessionContext, sql: &str) -> usize {
    let df = ctx.sql(sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let stream = execute_stream(plan, ctx.task_ctx()).unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    batches.iter().map(|b| b.num_rows()).sum()
}

fn bench_datafusion_sort(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("datafusion_sort");

    for size in SIZES.iter() {
        let batch = create_test_table(*size);
        let sql = "SELECT * FROM test_table ORDER BY id";

        // GPU benchmark
        group.bench_with_input(BenchmarkId::new("gpu_cudf", size), size, |b, _| {
            b.to_async(&rt).iter(|| async {
                let ctx = create_gpu_context(batch.clone()).await;
                let rows = execute_query(&ctx, black_box(sql)).await;
                black_box(rows)
            });
        });

        // CPU benchmark
        group.bench_with_input(BenchmarkId::new("cpu_datafusion", size), size, |b, _| {
            b.to_async(&rt).iter(|| async {
                let ctx = create_cpu_context(batch.clone()).await;
                let rows = execute_query(&ctx, black_box(sql)).await;
                black_box(rows)
            });
        });
    }
    group.finish();
}

fn bench_datafusion_filter(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("datafusion_filter");

    for size in SIZES.iter() {
        let batch = create_test_table(*size);
        // Filter ~50% of rows
        let threshold = *size as i64 / 2;
        let sql = format!("SELECT * FROM test_table WHERE id < {}", threshold);

        // GPU benchmark
        group.bench_with_input(BenchmarkId::new("gpu_cudf", size), size, |b, _| {
            let sql = sql.clone();
            b.to_async(&rt).iter(|| {
                let sql = sql.clone();
                let batch = batch.clone();
                async move {
                    let ctx = create_gpu_context(batch).await;
                    let rows = execute_query(&ctx, black_box(&sql)).await;
                    black_box(rows)
                }
            });
        });

        // CPU benchmark
        group.bench_with_input(BenchmarkId::new("cpu_datafusion", size), size, |b, _| {
            let sql = sql.clone();
            b.to_async(&rt).iter(|| {
                let sql = sql.clone();
                let batch = batch.clone();
                async move {
                    let ctx = create_cpu_context(batch).await;
                    let rows = execute_query(&ctx, black_box(&sql)).await;
                    black_box(rows)
                }
            });
        });
    }
    group.finish();
}

fn bench_datafusion_aggregate(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("datafusion_aggregate");

    for size in SIZES.iter() {
        let batch = create_test_table(*size);
        let sql = "SELECT group_key, SUM(value) FROM test_table GROUP BY group_key";

        // GPU benchmark
        group.bench_with_input(BenchmarkId::new("gpu_cudf", size), size, |b, _| {
            b.to_async(&rt).iter(|| async {
                let ctx = create_gpu_context(batch.clone()).await;
                let rows = execute_query(&ctx, black_box(sql)).await;
                black_box(rows)
            });
        });

        // CPU benchmark
        group.bench_with_input(BenchmarkId::new("cpu_datafusion", size), size, |b, _| {
            b.to_async(&rt).iter(|| async {
                let ctx = create_cpu_context(batch.clone()).await;
                let rows = execute_query(&ctx, black_box(sql)).await;
                black_box(rows)
            });
        });
    }
    group.finish();
}

fn bench_datafusion_complex(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("datafusion_complex");

    for size in SIZES.iter() {
        let batch = create_test_table(*size);
        // Complex query: filter + aggregate + sort
        let sql = "SELECT group_key, SUM(value) as total \
                   FROM test_table \
                   WHERE id > 0 \
                   GROUP BY group_key \
                   ORDER BY total DESC \
                   LIMIT 10";

        // GPU benchmark
        group.bench_with_input(BenchmarkId::new("gpu_cudf", size), size, |b, _| {
            b.to_async(&rt).iter(|| async {
                let ctx = create_gpu_context(batch.clone()).await;
                let rows = execute_query(&ctx, black_box(sql)).await;
                black_box(rows)
            });
        });

        // CPU benchmark
        group.bench_with_input(BenchmarkId::new("cpu_datafusion", size), size, |b, _| {
            b.to_async(&rt).iter(|| async {
                let ctx = create_cpu_context(batch.clone()).await;
                let rows = execute_query(&ctx, black_box(sql)).await;
                black_box(rows)
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_datafusion_sort,
    bench_datafusion_filter,
    bench_datafusion_aggregate,
    bench_datafusion_complex
);
criterion_main!(benches);
