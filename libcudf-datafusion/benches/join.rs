use arrow::array::{Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use libcudf_rs::{
    configure_default_pools, inner_join, left_join, left_semi_join, CuDFTable, CuDFTableView,
};
use std::hint::black_box;
use std::sync::Arc;

/// (left_rows, right_rows)
const SIZES: &[(usize, usize)] = &[
    (100_000, 50_000),
    (1_000_000, 150_000),
    (5_000_000, 150_000),
];

fn make_left(n: usize, right_key_range: usize) -> CuDFTable {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]));
    let keys: Int64Array = (0..n).map(|i| (i % right_key_range) as i64).collect();
    let a: Int32Array = (0..n).map(|i| i as i32).collect();
    let b: Int32Array = (0..n).map(|i| (i * 2) as i32).collect();
    CuDFTable::from_arrow_host(
        RecordBatch::try_new(schema, vec![Arc::new(keys), Arc::new(a), Arc::new(b)]).unwrap(),
    )
    .unwrap()
}

fn make_right(m: usize) -> CuDFTable {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("x", DataType::Int32, false),
        Field::new("y", DataType::Int32, false),
    ]));
    let keys: Int64Array = (0..m).map(|i| i as i64).collect();
    let x: Int32Array = (0..m).map(|i| i as i32).collect();
    let y: Int32Array = (0..m).map(|i| (i * 3) as i32).collect();
    CuDFTable::from_arrow_host(
        RecordBatch::try_new(schema, vec![Arc::new(keys), Arc::new(x), Arc::new(y)]).unwrap(),
    )
    .unwrap()
}

/// Upload both tables to GPU and return pre-built views.
fn gpu_views(left_n: usize, right_m: usize) -> (CuDFTableView, CuDFTableView) {
    let left = Arc::new(make_left(left_n, right_m));
    let right = Arc::new(make_right(right_m));
    (Arc::clone(&left).view(), Arc::clone(&right).view())
}

fn bench_inner_join(c: &mut Criterion) {
    configure_default_pools();
    let mut group = c.benchmark_group("join/inner");

    for &(left_n, right_m) in SIZES {
        let (lv, rv) = gpu_views(left_n, right_m);
        let id = format!("{left_n}x{right_m}");

        group.throughput(Throughput::Elements(left_n as u64));
        group.bench_with_input(BenchmarkId::new("all_cols", &id), &(), |b, _| {
            b.iter(|| {
                black_box(
                    inner_join(&lv, &rv, &[0], &[0], None, None)
                        .unwrap()
                        .num_rows(),
                )
            })
        });

        group.bench_with_input(BenchmarkId::new("projected_1col", &id), &(), |b, _| {
            b.iter(|| {
                black_box(
                    inner_join(&lv, &rv, &[0], &[0], Some(&[1]), Some(&[1]))
                        .unwrap()
                        .num_rows(),
                )
            })
        });
    }

    group.finish();
}

fn bench_left_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("join/left");

    for &(left_n, right_m) in SIZES {
        let (lv, rv) = gpu_views(left_n, right_m);

        group.throughput(Throughput::Elements(left_n as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{left_n}x{right_m}")),
            &(),
            |b, _| {
                b.iter(|| {
                    black_box(
                        left_join(&lv, &rv, &[0], &[0], None, None)
                            .unwrap()
                            .num_rows(),
                    )
                })
            },
        );
    }

    group.finish();
}

fn bench_semi_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("join/semi");

    for &(left_n, right_m) in SIZES {
        let (lv, rv) = gpu_views(left_n, right_m);

        group.throughput(Throughput::Elements(left_n as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{left_n}x{right_m}")),
            &(),
            |b, _| b.iter(|| black_box(left_semi_join(&lv, &rv, &[0], &[0]).unwrap().num_rows())),
        );
    }

    group.finish();
}

criterion_group!(benches, bench_inner_join, bench_left_join, bench_semi_join);
criterion_main!(benches);
