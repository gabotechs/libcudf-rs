use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use libcudf_rs::{AggregationOp, AggregationRequest, CuDFGroupBy, CuDFTable, CuDFTableView};
use std::collections::HashMap;
use std::hint::black_box;

use arrow::array::{Float64Array, Int32Array, Int64Array};

mod common;
use common::{create_groupby_batch, SIZES};

const NUM_GROUPS: usize = 100;

fn bench_groupby_sum(c: &mut Criterion) {
    let mut group = c.benchmark_group("groupby_sum");

    for size in SIZES.iter() {
        let batch = create_groupby_batch(*size, NUM_GROUPS);
        let bytes = batch.get_array_memory_size();
        group.throughput(Throughput::Bytes(bytes as u64));

        // GPU benchmark
        group.bench_with_input(BenchmarkId::new("gpu_cudf", size), size, |b, _| {
            b.iter(|| {
                let table = CuDFTable::from_arrow_host(batch.clone()).unwrap();
                let view = table.into_view();

                // Column 0 is group_key, column 1 is value1
                let key_col = view.column(0);
                let val_col = view.column(1);

                let keys_view = CuDFTableView::from_column_views(vec![key_col]).unwrap();
                let groupby = CuDFGroupBy::from_table_view(keys_view);

                let mut request = AggregationRequest::from_column_view(val_col);
                request.add(AggregationOp::SUM.group_by());

                let result = groupby.aggregate(black_box(&[request])).unwrap();
                black_box(result)
            });
        });

        // CPU benchmark using HashMap
        group.bench_with_input(BenchmarkId::new("cpu_hashmap", size), size, |b, _| {
            let key_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let val_array = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            b.iter(|| {
                let mut sums: HashMap<i32, i64> = HashMap::new();
                for i in 0..key_array.len() {
                    let key = key_array.value(i);
                    let val = val_array.value(i);
                    *sums.entry(key).or_insert(0) += val;
                }
                black_box(sums)
            });
        });
    }
    group.finish();
}

fn bench_groupby_multi_agg(c: &mut Criterion) {
    let mut group = c.benchmark_group("groupby_multi_agg");

    for size in SIZES.iter() {
        let batch = create_groupby_batch(*size, NUM_GROUPS);
        let bytes = batch.get_array_memory_size();
        group.throughput(Throughput::Bytes(bytes as u64));

        // GPU benchmark: SUM, MIN, MAX, COUNT
        group.bench_with_input(BenchmarkId::new("gpu_cudf", size), size, |b, _| {
            b.iter(|| {
                let table = CuDFTable::from_arrow_host(batch.clone()).unwrap();
                let view = table.into_view();

                let key_col = view.column(0);
                let val_col = view.column(1);

                let keys_view = CuDFTableView::from_column_views(vec![key_col]).unwrap();
                let groupby = CuDFGroupBy::from_table_view(keys_view);

                let mut request = AggregationRequest::from_column_view(val_col);
                request.add(AggregationOp::SUM.group_by());
                request.add(AggregationOp::MIN.group_by());
                request.add(AggregationOp::MAX.group_by());
                request.add(AggregationOp::COUNT.group_by());

                let result = groupby.aggregate(black_box(&[request])).unwrap();
                black_box(result)
            });
        });

        // CPU benchmark with multiple aggregations
        group.bench_with_input(BenchmarkId::new("cpu_hashmap", size), size, |b, _| {
            let key_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let val_array = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            b.iter(|| {
                // (sum, min, max, count)
                let mut aggs: HashMap<i32, (i64, i64, i64, usize)> = HashMap::new();
                for i in 0..key_array.len() {
                    let key = key_array.value(i);
                    let val = val_array.value(i);
                    let entry = aggs.entry(key).or_insert((0, i64::MAX, i64::MIN, 0));
                    entry.0 += val;
                    entry.1 = entry.1.min(val);
                    entry.2 = entry.2.max(val);
                    entry.3 += 1;
                }
                black_box(aggs)
            });
        });
    }
    group.finish();
}

fn bench_groupby_mean(c: &mut Criterion) {
    let mut group = c.benchmark_group("groupby_mean");

    for size in SIZES.iter() {
        let batch = create_groupby_batch(*size, NUM_GROUPS);
        let bytes = batch.get_array_memory_size();
        group.throughput(Throughput::Bytes(bytes as u64));

        // GPU benchmark
        group.bench_with_input(BenchmarkId::new("gpu_cudf", size), size, |b, _| {
            b.iter(|| {
                let table = CuDFTable::from_arrow_host(batch.clone()).unwrap();
                let view = table.into_view();

                let key_col = view.column(0);
                let val_col = view.column(2); // value2 is Float64

                let keys_view = CuDFTableView::from_column_views(vec![key_col]).unwrap();
                let groupby = CuDFGroupBy::from_table_view(keys_view);

                let mut request = AggregationRequest::from_column_view(val_col);
                request.add(AggregationOp::MEAN.group_by());

                let result = groupby.aggregate(black_box(&[request])).unwrap();
                black_box(result)
            });
        });

        // CPU benchmark
        group.bench_with_input(BenchmarkId::new("cpu_hashmap", size), size, |b, _| {
            let key_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let val_array = batch
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();

            b.iter(|| {
                let mut sums: HashMap<i32, (f64, usize)> = HashMap::new();
                for i in 0..key_array.len() {
                    let key = key_array.value(i);
                    let val = val_array.value(i);
                    let entry = sums.entry(key).or_insert((0.0, 0));
                    entry.0 += val;
                    entry.1 += 1;
                }
                // Compute means
                let means: HashMap<i32, f64> = sums
                    .into_iter()
                    .map(|(k, (sum, count))| (k, sum / count as f64))
                    .collect();
                black_box(means)
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_groupby_sum, bench_groupby_multi_agg, bench_groupby_mean);
criterion_main!(benches);
