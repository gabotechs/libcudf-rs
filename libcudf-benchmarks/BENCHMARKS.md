# Benchmark Results: GPU (cuDF) vs CPU

Benchmarks comparing cuDF GPU-accelerated operations against CPU-based Arrow compute kernels.

**Test Environment:**
- GPU: NVIDIA Tesla T4 (AWS g4dn.xlarge)
- CPU: Intel Xeon (4 vCPUs)
- OS: Ubuntu 24.04
- CUDA: 12.6
- Data sizes: 10K, 100K, 1M rows

## Sort Benchmarks

| Operation | Size | GPU (cuDF) | CPU (Arrow) | Speedup |
|-----------|------|------------|-------------|---------|
| Single column | 10K | 576µs | 267µs | 0.5x (CPU faster) |
| Single column | 100K | 1.97ms | 3.23ms | **1.6x** |
| Single column | 1M | 13.5ms | 40.7ms | **3.0x** |
| Multi column | 10K | 821µs | 641µs | 0.8x |
| Multi column | 100K | 2.38ms | 8.13ms | **3.4x** |
| Multi column | 1M | 18.0ms | 128ms | **7.1x** |

## GroupBy Benchmarks

| Operation | Size | GPU (cuDF) | CPU (HashMap) | Speedup |
|-----------|------|------------|---------------|---------|
| SUM | 10K | 566µs | 164µs | 0.3x |
| SUM | 100K | 1.27ms | 1.62ms | **1.3x** |
| SUM | 1M | 6.8ms | 16.2ms | **2.4x** |
| Multi-agg | 10K | 623µs | 224µs | 0.4x |
| Multi-agg | 100K | 1.59ms | 2.21ms | **1.4x** |
| Multi-agg | 1M | 7.5ms | 22.1ms | **2.9x** |
| MEAN | 10K | 573µs | 170µs | 0.3x |
| MEAN | 100K | 1.32ms | 1.65ms | **1.3x** |
| MEAN | 1M | 6.9ms | 16.5ms | **2.4x** |

*Multi-agg = SUM + MIN + MAX + COUNT computed together*

## Filter Benchmarks

| Selectivity | Size | GPU (cuDF) | CPU (Arrow) | Speedup |
|-------------|------|------------|-------------|---------|
| 50% | 10K | 552µs | 57µs | 0.1x |
| 50% | 100K | 1.63ms | 622µs | 0.4x |
| 50% | 1M | 10.4ms | 7.3ms | 0.7x |

*Note: Filter benchmarks include GPU memory transfer overhead (host-to-device). For data already on GPU, filter performance would be significantly better.*

## Key Findings

1. **GPU excels at compute-intensive operations** - Sorting (especially multi-column) and aggregations show 2-7x speedups at scale.

2. **Crossover point ~100K rows** - Below this, CPU is often faster due to GPU transfer overhead.

3. **Simple operations favor CPU** - Filtering is memory-bound and the GPU transfer cost dominates.

4. **GPU advantage grows with data size** - The larger the dataset, the more GPU parallelism helps.

## DataFusion Integration Benchmarks

Comparing DataFusion queries with GPU acceleration (via libcudf-datafusion) against CPU-only execution.

| Query Type | Size | GPU (cuDF) | CPU (DataFusion) | Speedup |
|------------|------|------------|------------------|---------|
| Sort | 10K | 1.11ms | 1.07ms | ~1.0x |
| Sort | 100K | 7.33ms | 7.33ms | ~1.0x |
| Sort | 1M | 128.9ms | 130.1ms | ~1.0x |
| Filter | 10K | 1.00ms | 1.00ms | ~1.0x |
| Filter | 100K | 1.27ms | 1.27ms | ~1.0x |
| Filter | 1M | 4.01ms | 4.02ms | ~1.0x |
| Aggregate | 10K | 1.41ms | 1.42ms | ~1.0x |
| Aggregate | 100K | 1.77ms | 1.77ms | ~1.0x |
| Aggregate | 1M | 5.53ms | 5.52ms | ~1.0x |
| Complex* | 10K | 2.55ms | 2.55ms | ~1.0x |
| Complex* | 100K | 3.00ms | 3.01ms | ~1.0x |
| Complex* | 1M | 7.41ms | 7.49ms | ~1.0x |

*Complex = Filter + Aggregate + Sort + LIMIT

**Note:** DataFusion integration shows similar performance because:
1. Data transfer overhead (CPU↔GPU) is included in each query
2. DataFusion's query planning adds overhead that masks GPU benefits
3. For end-to-end queries starting from CPU data, the raw cuDF speedups are offset by transfer costs

For workloads where data is already on GPU, or for very large datasets where compute dominates transfer time, GPU acceleration provides more benefit.

## Running Benchmarks

```bash
# Run all benchmarks
cargo bench --package libcudf-benchmarks

# Run specific benchmark
cargo bench --package libcudf-benchmarks --bench sort_benchmark
cargo bench --package libcudf-benchmarks --bench groupby_benchmark
cargo bench --package libcudf-benchmarks --bench filter_benchmark
cargo bench --package libcudf-benchmarks --bench datafusion_benchmark

# View HTML reports
open target/criterion/report/index.html
```
