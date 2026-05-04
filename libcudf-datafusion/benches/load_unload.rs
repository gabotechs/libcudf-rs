use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use datafusion::execution::TaskContext;
use datafusion_physical_plan::test::TestMemoryExec;
use datafusion_physical_plan::{execute_stream, ExecutionPlan};
use futures_util::TryStreamExt;
use libcudf_datafusion::{configure_default_pools, CuDFLoadExec, CuDFUnloadExec};
use std::hint::black_box;
use std::sync::Arc;
use tokio::runtime::Runtime;

const SIZES: &[usize] = &[100_000, 1_000_000, 10_000_000];

fn make_cpu_batches(total_rows: usize) -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("orderkey", DataType::Int64, false),
        Field::new("custkey", DataType::Int64, false),
        Field::new("orderstatus", DataType::Utf8, false),
        Field::new("totalprice", DataType::Float64, false),
        Field::new("orderdate", DataType::Int32, false),
        Field::new("orderpriority", DataType::Utf8, false),
        Field::new("clerk", DataType::Utf8, false),
        Field::new("shippriority", DataType::Int32, false),
        Field::new("comment", DataType::Utf8, false),
    ]));
    let batch_size = 8192;
    let mut batches = Vec::new();
    let mut offset = 0;
    while offset < total_rows {
        let len = batch_size.min(total_rows - offset);
        batches.push(
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from_iter_values(0..len as i64)),
                    Arc::new(Int64Array::from_iter_values(0..len as i64)),
                    Arc::new(StringArray::from(vec!["O"; len])),
                    Arc::new(Float64Array::from_iter_values((0..len).map(|i| i as f64))),
                    Arc::new(Int32Array::from_iter_values(0..len as i32)),
                    Arc::new(StringArray::from(vec!["3-MEDIUM"; len])),
                    Arc::new(StringArray::from(vec!["Clerk#000000001"; len])),
                    Arc::new(Int32Array::from_iter_values(vec![0i32; len])),
                    Arc::new(StringArray::from(vec!["comment"; len])),
                ],
            )
            .unwrap(),
        );
        offset += len;
    }
    batches
}

fn load_plan(batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
    let schema = batches[0].schema();
    let mem = Arc::new(TestMemoryExec::try_new(&[batches], schema, None).unwrap());
    Arc::new(CuDFLoadExec::try_new(mem).unwrap())
}

fn unload_plan(gpu_batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
    let schema = gpu_batches[0].schema();
    let mem = Arc::new(TestMemoryExec::try_new(&[gpu_batches], schema, None).unwrap());
    Arc::new(CuDFUnloadExec::new(mem))
}

async fn drain(plan: Arc<dyn ExecutionPlan>) {
    let ctx = Arc::new(TaskContext::default());
    black_box(
        execute_stream(plan, ctx)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap(),
    );
}

fn gpu_batches(rt: &Runtime, total_rows: usize) -> Vec<RecordBatch> {
    rt.block_on(async {
        let ctx = Arc::new(TaskContext::default());
        execute_stream(load_plan(make_cpu_batches(total_rows)), ctx)
            .unwrap()
            .try_collect()
            .await
            .unwrap()
    })
}

// Unpooled benchmarks must run first, pools are one-time global state.

fn bench_load_unpooled(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("load");
    for &total_rows in SIZES {
        let batches = make_cpu_batches(total_rows);
        let bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
        group.throughput(Throughput::Bytes(bytes as u64));
        group.bench_with_input(
            BenchmarkId::new("unpooled", total_rows),
            &total_rows,
            |b, _| {
                b.iter_batched(
                    || load_plan(batches.clone()),
                    |plan| rt.block_on(drain(plan)),
                    BatchSize::PerIteration,
                );
            },
        );
    }
    group.finish();
}

fn bench_unload_unpooled(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("unload");
    for &total_rows in SIZES {
        let batches = gpu_batches(&rt, total_rows);
        let bytes: usize = make_cpu_batches(total_rows)
            .iter()
            .map(|b| b.get_array_memory_size())
            .sum();
        group.throughput(Throughput::Bytes(bytes as u64));
        group.bench_with_input(
            BenchmarkId::new("unpooled", total_rows),
            &total_rows,
            |b, _| {
                b.iter_batched(
                    || unload_plan(batches.clone()),
                    |plan| rt.block_on(drain(plan)),
                    BatchSize::PerIteration,
                );
            },
        );
    }
    group.finish();
}

fn bench_load_pooled(c: &mut Criterion) {
    configure_default_pools();
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("load");
    for &total_rows in SIZES {
        let batches = make_cpu_batches(total_rows);
        let bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
        group.throughput(Throughput::Bytes(bytes as u64));
        group.bench_with_input(
            BenchmarkId::new("pooled", total_rows),
            &total_rows,
            |b, _| {
                b.iter_batched(
                    || load_plan(batches.clone()),
                    |plan| rt.block_on(drain(plan)),
                    BatchSize::PerIteration,
                );
            },
        );
    }
    group.finish();
}

fn bench_unload_pooled(c: &mut Criterion) {
    // pools already configured by bench_load_pooled
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("unload");
    for &total_rows in SIZES {
        let batches = gpu_batches(&rt, total_rows);
        let bytes: usize = make_cpu_batches(total_rows)
            .iter()
            .map(|b| b.get_array_memory_size())
            .sum();
        group.throughput(Throughput::Bytes(bytes as u64));
        group.bench_with_input(
            BenchmarkId::new("pooled", total_rows),
            &total_rows,
            |b, _| {
                b.iter_batched(
                    || unload_plan(batches.clone()),
                    |plan| rt.block_on(drain(plan)),
                    BatchSize::PerIteration,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_load_unpooled,
    bench_unload_unpooled,
    bench_load_pooled,
    bench_unload_pooled
);
criterion_main!(benches);
