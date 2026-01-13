use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::fs;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use arrow::record_batch::RecordBatch;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_physical_plan::execute_stream;
use futures_util::TryStreamExt;
use libcudf_datafusion::{CuDFConfig, HostToCuDFRule};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tokio::runtime::Runtime;
use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
    PartSuppGenerator, RegionGenerator, SupplierGenerator,
};
use tpchgen_arrow::{
    CustomerArrow, LineItemArrow, NationArrow, OrderArrow, PartArrow, PartSuppArrow, RegionArrow,
    SupplierArrow,
};

// Scale factor 10 produces ~10GB of data
const SCALE_FACTOR: f64 = 10.0;
const DATA_PARTS: i32 = 16;

// All 22 TPC-H queries
const BENCHMARK_QUERIES: &[(u8, &str)] = &[
    (1, "Q01"),
    (2, "Q02"),
    (3, "Q03"),
    (4, "Q04"),
    (5, "Q05"),
    (6, "Q06"),
    (7, "Q07"),
    (8, "Q08"),
    (9, "Q09"),
    (10, "Q10"),
    (11, "Q11"),
    (12, "Q12"),
    (13, "Q13"),
    (14, "Q14"),
    (15, "Q15"),
    (16, "Q16"),
    (17, "Q17"),
    (18, "Q18"),
    (19, "Q19"),
    (20, "Q20"),
    (21, "Q21"),
    (22, "Q22"),
];

static TPCH_DATA_DIR: OnceLock<PathBuf> = OnceLock::new();

fn get_data_dir() -> &'static PathBuf {
    TPCH_DATA_DIR.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("libcudf-benchmarks-tpch-sf{}", SCALE_FACTOR as i32));
        if !dir.exists() {
            eprintln!("Generating TPC-H SF={} data (~{}GB)...", SCALE_FACTOR, SCALE_FACTOR as i32);
            generate_tpch_data(&dir, SCALE_FACTOR, DATA_PARTS);
            eprintln!("TPC-H data generation complete.");
        }
        dir
    })
}

fn generate_table<A>(
    mut data_source: A,
    table_name: &str,
    data_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>>
where
    A: Iterator<Item = RecordBatch>,
{
    let output_path = data_dir.join(format!("{table_name}.parquet"));

    if let Some(first_batch) = data_source.next() {
        let file = fs::File::create(&output_path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, first_batch.schema(), Some(props))?;

        writer.write(&first_batch)?;

        for batch in data_source {
            writer.write(&batch)?;
        }

        writer.close()?;
    }

    Ok(())
}

fn generate_tpch_data(data_dir: &Path, sf: f64, parts: i32) {
    fs::create_dir_all(data_dir).expect("Failed to create data directory");

    macro_rules! must_generate_tpch_table {
        ($generator:ident, $arrow:ident, $name:literal) => {
            let table_dir = data_dir.join($name);
            fs::create_dir_all(&table_dir).expect("Failed to create data directory");
            (1..=parts).for_each(|part| {
                generate_table(
                    $arrow::new($generator::new(sf, part, parts)).with_batch_size(8192),
                    &format!("{part}"),
                    &table_dir,
                )
                .expect(concat!("Failed to generate ", $name, " table"));
            });
        };
    }

    must_generate_tpch_table!(RegionGenerator, RegionArrow, "region");
    must_generate_tpch_table!(NationGenerator, NationArrow, "nation");
    must_generate_tpch_table!(CustomerGenerator, CustomerArrow, "customer");
    must_generate_tpch_table!(SupplierGenerator, SupplierArrow, "supplier");
    must_generate_tpch_table!(PartGenerator, PartArrow, "part");
    must_generate_tpch_table!(PartSuppGenerator, PartSuppArrow, "partsupp");
    must_generate_tpch_table!(OrderGenerator, OrderArrow, "orders");
    must_generate_tpch_table!(LineItemGenerator, LineItemArrow, "lineitem");
}

fn get_query(num: u8) -> String {
    let queries_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../libcudf-datafusion/testdata/tpch/queries");
    let query_path = queries_dir.join(format!("q{num}.sql"));
    fs::read_to_string(query_path)
        .unwrap_or_else(|_| panic!("Failed to read TPCH query file: q{num}.sql"))
        .trim()
        .to_string()
}

async fn create_gpu_context() -> SessionContext {
    let config = SessionConfig::new().with_option_extension(CuDFConfig::default());
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .with_physical_optimizer_rule(Arc::new(HostToCuDFRule))
        .build();
    let ctx = SessionContext::from(state);
    register_tables(&ctx).await;
    ctx
}

async fn create_cpu_context() -> SessionContext {
    let ctx = SessionContext::new();
    register_tables(&ctx).await;
    ctx
}

async fn register_tables(ctx: &SessionContext) {
    let data_dir = get_data_dir();

    for table_name in [
        "lineitem", "orders", "part", "partsupp", "customer", "nation", "region", "supplier",
    ] {
        let table_path = data_dir.join(table_name);
        ctx.register_parquet(
            table_name,
            table_path.to_string_lossy().as_ref(),
            datafusion::prelude::ParquetReadOptions::default(),
        )
        .await
        .expect("Failed to register table");
    }
}

async fn execute_query(ctx: &SessionContext, sql: &str) -> usize {
    // Q15 has three statements: CREATE VIEW, SELECT, DROP VIEW
    if sql.to_lowercase().starts_with("create view") {
        let queries: Vec<&str> = sql
            .split(';')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();

        ctx.sql(queries[0]).await.unwrap().collect().await.unwrap();
        let df = ctx.sql(queries[1]).await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let stream = execute_stream(plan, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        ctx.sql(queries[2]).await.unwrap().collect().await.unwrap();
        return batches.iter().map(|b| b.num_rows()).sum();
    }

    let df = ctx.sql(sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let stream = execute_stream(plan, ctx.task_ctx()).unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    batches.iter().map(|b| b.num_rows()).sum()
}

fn bench_tpch(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Ensure data is generated before starting benchmarks
    rt.block_on(async {
        let _ = get_data_dir();
    });

    let mut group = c.benchmark_group("tpch");
    // Configure for longer-running queries
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(30));

    for (query_num, query_name) in BENCHMARK_QUERIES {
        let sql = get_query(*query_num);

        // GPU benchmark
        group.bench_with_input(
            BenchmarkId::new("gpu_cudf", query_name),
            query_name,
            |b, _| {
                let sql = sql.clone();
                b.to_async(&rt).iter(|| {
                    let sql = sql.clone();
                    async move {
                        let ctx = create_gpu_context().await;
                        let rows = execute_query(&ctx, black_box(&sql)).await;
                        black_box(rows)
                    }
                });
            },
        );

        // CPU benchmark
        group.bench_with_input(
            BenchmarkId::new("cpu_datafusion", query_name),
            query_name,
            |b, _| {
                let sql = sql.clone();
                b.to_async(&rt).iter(|| {
                    let sql = sql.clone();
                    async move {
                        let ctx = create_cpu_context().await;
                        let rows = execute_query(&ctx, black_box(&sql)).await;
                        black_box(rows)
                    }
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_tpch);
criterion_main!(benches);
