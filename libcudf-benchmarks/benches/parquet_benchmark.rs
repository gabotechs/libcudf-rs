use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use libcudf_rs::CuDFTable;
use std::fs::File;
use std::hint::black_box;
use tempfile::NamedTempFile;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;

mod common;
use common::{create_numeric_batch, SIZES};

fn bench_parquet_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("parquet_read");

    for size in SIZES.iter() {
        let batch = create_numeric_batch(*size);

        // Write test file once before benchmarking
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();

        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let file_size = std::fs::metadata(&path).unwrap().len();
        group.throughput(Throughput::Bytes(file_size));

        // GPU read
        group.bench_with_input(BenchmarkId::new("gpu_cudf", size), size, |b, _| {
            b.iter(|| {
                let table = CuDFTable::from_parquet(black_box(&path)).unwrap();
                black_box(table)
            });
        });

        // CPU read (Arrow/Parquet)
        group.bench_with_input(BenchmarkId::new("cpu_arrow", size), size, |b, _| {
            b.iter(|| {
                let file = File::open(black_box(&path)).unwrap();
                let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
                let reader = builder.build().unwrap();
                let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
                black_box(batches)
            });
        });

        // Keep temp file alive until benchmarks complete
        drop(temp_file);
    }
    group.finish();
}

fn bench_parquet_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("parquet_write");

    for size in SIZES.iter() {
        let batch = create_numeric_batch(*size);
        let bytes = batch.get_array_memory_size();
        group.throughput(Throughput::Bytes(bytes as u64));

        // GPU write
        group.bench_with_input(BenchmarkId::new("gpu_cudf", size), size, |b, _| {
            let temp_file = NamedTempFile::new().unwrap();
            let path = temp_file.path().to_str().unwrap().to_string();
            b.iter(|| {
                let table = CuDFTable::from_arrow_host(batch.clone()).unwrap();
                table.to_parquet(black_box(&path)).unwrap();
            });
        });

        // CPU write
        group.bench_with_input(BenchmarkId::new("cpu_arrow", size), size, |b, _| {
            let temp_file = NamedTempFile::new().unwrap();
            let path = temp_file.path().to_str().unwrap().to_string();
            let schema = batch.schema();
            b.iter(|| {
                let file = File::create(black_box(&path)).unwrap();
                let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
                writer.write(&batch).unwrap();
                writer.close().unwrap();
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_parquet_read, bench_parquet_write);
criterion_main!(benches);
