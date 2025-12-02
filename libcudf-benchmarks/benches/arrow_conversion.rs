use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use libcudf_rs::CuDFTable;
use std::hint::black_box;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

fn bench_arrow_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("arrow_roundtrip");

    for size in [1_000, 10_000, 100_000].iter() {
        let batch = create_test_batch(*size);
        let bytes = batch.get_array_memory_size();

        group.throughput(Throughput::Bytes((bytes * 2) as u64)); // Both conversions
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &_size| {
            b.iter(|| {
                let table = CuDFTable::from_arrow(black_box(batch.clone())).unwrap();
                let result = table.to_arrow().unwrap();
                black_box(result)
            });
        });
    }
    group.finish();
}

fn create_test_batch(num_rows: usize) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("int32", DataType::Int32, false),
        Field::new("int64", DataType::Int64, false),
        Field::new("float64", DataType::Float64, false),
        Field::new("string", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]);

    let int32_data: Vec<i32> = (0..num_rows as i32).collect();
    let int64_data: Vec<i64> = (0..num_rows as i64).map(|x| x * 1000).collect();
    let float64_data: Vec<f64> = (0..num_rows).map(|x| x as f64 * 1.5).collect();
    let string_data: Vec<String> = (0..num_rows).map(|x| format!("row_{}", x)).collect();
    let timestamp_data: Vec<i64> = (0..num_rows as i64).map(|x| x * 1000).collect();

    let arrays: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(Int32Array::from(int32_data)),
        Arc::new(Int64Array::from(int64_data)),
        Arc::new(Float64Array::from(float64_data)),
        Arc::new(StringArray::from(string_data)),
        Arc::new(TimestampMillisecondArray::from(timestamp_data)),
    ];

    RecordBatch::try_new(Arc::new(schema), arrays).unwrap()
}

criterion_group!(benches, bench_arrow_roundtrip);
criterion_main!(benches);
