use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use libcudf_rs::{sort, CuDFTable, SortOrder};
use std::hint::black_box;

use arrow::compute::{lexsort_to_indices, sort_to_indices, take, SortColumn, SortOptions};

mod common;
use common::{create_numeric_batch, SIZES};

fn bench_sort_single_column(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_single_column");

    for size in SIZES.iter() {
        let batch = create_numeric_batch(*size);
        let bytes = batch.get_array_memory_size();
        group.throughput(Throughput::Bytes(bytes as u64));

        // GPU benchmark
        group.bench_with_input(BenchmarkId::new("gpu_cudf", size), size, |b, _| {
            b.iter(|| {
                let table = CuDFTable::from_arrow_host(batch.clone()).unwrap();
                let view = table.into_view();
                let sorted = sort(black_box(&view), &[0], &[SortOrder::AscendingNullsLast]).unwrap();
                black_box(sorted)
            });
        });

        // CPU benchmark (Arrow)
        group.bench_with_input(BenchmarkId::new("cpu_arrow", size), size, |b, _| {
            let col = batch.column(0).clone();
            b.iter(|| {
                let indices = sort_to_indices(black_box(&col), None, None).unwrap();
                let sorted = take(col.as_ref(), &indices, None).unwrap();
                black_box(sorted)
            });
        });
    }
    group.finish();
}

fn bench_sort_multi_column(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_multi_column");

    for size in SIZES.iter() {
        let batch = create_numeric_batch(*size);
        let bytes = batch.get_array_memory_size();
        group.throughput(Throughput::Bytes(bytes as u64));

        // GPU benchmark - sort by columns 0 (asc) and 1 (desc)
        group.bench_with_input(BenchmarkId::new("gpu_cudf", size), size, |b, _| {
            b.iter(|| {
                let table = CuDFTable::from_arrow_host(batch.clone()).unwrap();
                let view = table.into_view();
                let sorted = sort(
                    black_box(&view),
                    &[0, 1],
                    &[SortOrder::AscendingNullsLast, SortOrder::DescendingNullsFirst],
                )
                .unwrap();
                black_box(sorted)
            });
        });

        // CPU benchmark using lexsort_to_indices
        group.bench_with_input(BenchmarkId::new("cpu_arrow", size), size, |b, _| {
            let cols: Vec<SortColumn> = vec![
                SortColumn {
                    values: batch.column(0).clone(),
                    options: None,
                },
                SortColumn {
                    values: batch.column(1).clone(),
                    options: Some(SortOptions {
                        descending: true,
                        nulls_first: true,
                    }),
                },
            ];
            b.iter(|| {
                let indices = lexsort_to_indices(black_box(&cols), None).unwrap();
                black_box(indices)
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_sort_single_column, bench_sort_multi_column);
criterion_main!(benches);
