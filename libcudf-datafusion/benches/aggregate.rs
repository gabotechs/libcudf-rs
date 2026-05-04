use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_physical_plan::{execute_stream, ExecutionPlan};
use futures_util::TryStreamExt;
use libcudf_datafusion::aggregate::{avg, count, max, min, sum};
use libcudf_datafusion::{CuDFConfig, HostToCuDFRule};
use std::hint::black_box;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("val", DataType::Float64, false),
    ]))
}

fn make_batches(n: usize) -> Vec<RecordBatch> {
    let schema = schema();
    let batch_size = 8192;
    let mut batches = Vec::new();
    let mut offset = 0;
    while offset < n {
        let len = batch_size.min(n - offset);
        let keys: Int64Array = (offset..offset + len)
            .map(|i| (i % 100_000) as i64)
            .collect();
        let vals: Float64Array = (offset..offset + len).map(|i| i as f64 * 0.7).collect();
        batches.push(
            RecordBatch::try_new(schema.clone(), vec![Arc::new(keys), Arc::new(vals)]).unwrap(),
        );
        offset += len;
    }
    batches
}

async fn cpu_ctx(batches: Vec<RecordBatch>) -> SessionContext {
    let schema = schema();
    let ctx = SessionContext::new();
    ctx.register_table(
        "t",
        Arc::new(MemTable::try_new(schema, vec![batches]).unwrap()),
    )
    .unwrap();
    ctx
}

async fn gpu_ctx(batches: Vec<RecordBatch>) -> SessionContext {
    let schema = schema();
    let mut cudf_config = CuDFConfig::default();
    cudf_config.enable = true;
    let config = SessionConfig::new().with_option_extension(cudf_config);
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .with_physical_optimizer_rule(Arc::new(HostToCuDFRule))
        .build();
    let ctx = SessionContext::from(state);
    ctx.register_udaf((*avg()).clone());
    ctx.register_udaf((*count()).clone());
    ctx.register_udaf((*max()).clone());
    ctx.register_udaf((*min()).clone());
    ctx.register_udaf((*sum()).clone());
    ctx.register_table(
        "t",
        Arc::new(MemTable::try_new(schema, vec![batches]).unwrap()),
    )
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

fn bench_group(c: &mut Criterion, group_name: &str, sql: &str) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group(group_name);

    for &n in &[1_000_000usize, 5_000_000, 20_000_000] {
        group.throughput(Throughput::Elements(n as u64));

        let batches = make_batches(n);
        let cpu = rt.block_on(cpu_ctx(batches.clone()));
        let gpu = rt.block_on(gpu_ctx(batches));

        let cpu_task_ctx = cpu.task_ctx();
        let gpu_task_ctx = gpu.task_ctx();

        group.bench_with_input(BenchmarkId::new("cpu", n), &n, |b, _| {
            b.iter_batched(
                || rt.block_on(build_plan(&cpu, sql)),
                |plan| {
                    rt.block_on(async {
                        let stream = execute_stream(plan, cpu_task_ctx.clone()).unwrap();
                        black_box(stream.try_collect::<Vec<_>>().await.unwrap());
                    })
                },
                BatchSize::PerIteration,
            );
        });
        group.bench_with_input(BenchmarkId::new("gpu", n), &n, |b, _| {
            b.iter_batched(
                || rt.block_on(build_plan(&gpu, sql)),
                |plan| {
                    rt.block_on(async {
                        let stream = execute_stream(plan, gpu_task_ctx.clone()).unwrap();
                        black_box(stream.try_collect::<Vec<_>>().await.unwrap());
                    })
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

fn bench_sum(c: &mut Criterion) {
    bench_group(c, "sum", "SELECT key, SUM(val) FROM t GROUP BY key");
}

fn bench_count(c: &mut Criterion) {
    bench_group(c, "count", "SELECT key, COUNT(val) FROM t GROUP BY key");
}

fn bench_avg(c: &mut Criterion) {
    bench_group(c, "avg", "SELECT key, AVG(val) FROM t GROUP BY key");
}

fn bench_min_max(c: &mut Criterion) {
    bench_group(
        c,
        "min_max",
        "SELECT key, MIN(val), MAX(val) FROM t GROUP BY key",
    );
}

fn bench_combined(c: &mut Criterion) {
    bench_group(
        c,
        "combined",
        "SELECT key, COUNT(val), SUM(val), AVG(val), MIN(val), MAX(val) FROM t GROUP BY key",
    );
}

criterion_group!(
    benches,
    bench_sum,
    bench_count,
    bench_avg,
    bench_min_max,
    bench_combined
);
criterion_main!(benches);
