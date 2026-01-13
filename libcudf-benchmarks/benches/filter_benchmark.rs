use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use libcudf_rs::{apply_boolean_mask, CuDFColumn, CuDFTable};
use std::hint::black_box;
use std::sync::Arc;

use arrow::compute::filter_record_batch;

mod common;
use common::{create_boolean_mask, create_numeric_batch, SIZES};

fn bench_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter");
    let selectivity = 0.5; // 50% selectivity

    for size in SIZES.iter() {
        let batch = create_numeric_batch(*size);
        let mask = create_boolean_mask(*size, selectivity);
        let bytes = batch.get_array_memory_size();
        group.throughput(Throughput::Bytes(bytes as u64));

        // GPU benchmark
        group.bench_with_input(BenchmarkId::new("gpu_cudf", size), size, |b, _| {
            b.iter(|| {
                let table = CuDFTable::from_arrow_host(batch.clone()).unwrap();
                let view = table.into_view();
                let mask_col = CuDFColumn::from_arrow_host(&mask).unwrap();
                let mask_arc = Arc::new(mask_col);
                let mask_view = mask_arc.view();
                let filtered = apply_boolean_mask(black_box(&view), black_box(&mask_view)).unwrap();
                black_box(filtered)
            });
        });

        // CPU benchmark (Arrow)
        group.bench_with_input(BenchmarkId::new("cpu_arrow", size), size, |b, _| {
            b.iter(|| {
                let filtered = filter_record_batch(black_box(&batch), black_box(&mask)).unwrap();
                black_box(filtered)
            });
        });
    }
    group.finish();
}

fn bench_filter_selectivity(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_selectivity");
    let size = 100_000;
    let selectivities = [0.01, 0.1, 0.5, 0.9];

    let batch = create_numeric_batch(size);
    let bytes = batch.get_array_memory_size();
    group.throughput(Throughput::Bytes(bytes as u64));

    for selectivity in selectivities.iter() {
        let mask = create_boolean_mask(size, *selectivity);
        let pct = (*selectivity * 100.0) as i32;

        // GPU
        group.bench_with_input(
            BenchmarkId::new(format!("gpu_cudf_{}pct", pct), selectivity),
            selectivity,
            |b, _| {
                b.iter(|| {
                    let table = CuDFTable::from_arrow_host(batch.clone()).unwrap();
                    let view = table.into_view();
                    let mask_col = CuDFColumn::from_arrow_host(&mask).unwrap();
                    let mask_arc = Arc::new(mask_col);
                    let mask_view = mask_arc.view();
                    let filtered =
                        apply_boolean_mask(black_box(&view), black_box(&mask_view)).unwrap();
                    black_box(filtered)
                });
            },
        );

        // CPU
        group.bench_with_input(
            BenchmarkId::new(format!("cpu_arrow_{}pct", pct), selectivity),
            selectivity,
            |b, _| {
                b.iter(|| {
                    let filtered = filter_record_batch(black_box(&batch), black_box(&mask)).unwrap();
                    black_box(filtered)
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_filter, bench_filter_selectivity);
criterion_main!(benches);
