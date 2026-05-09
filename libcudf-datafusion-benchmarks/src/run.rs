// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::results::{BenchResult, BenchmarkRun, QueryIter};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::instant::Instant;
use datafusion::common::utils::get_available_parallelism;
use datafusion::common::{exec_err, not_impl_err};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::*;
use libcudf_datafusion::aggregate::{avg, count, max, min, sum};
use libcudf_datafusion::{CuDFConfig, SessionStateBuilderExt};
use libcudf_datafusion_benchmarks::datasets::{
    apply_query_settings, clickbench, register_tables, tpcds, tpch,
};
use std::fs;
use std::path::PathBuf;
use std::time::Duration;
use structopt::StructOpt;

/// Run the benchmarks.
///
/// Supports the TPC-H, TPC-DS and ClickBench datasets. The dataset is
/// chosen by the `--dataset` flag and is expected to be a directory under
/// `data/` produced by one of the `prepare-*` subcommands.
#[derive(Debug, StructOpt, Clone)]
#[structopt(verbatim_doc_comment)]
pub struct RunOpt {
    /// Query number. If not specified, runs all queries
    #[structopt(short, long, use_delimiter = true)]
    pub query: Vec<String>,

    /// Path to data files
    #[structopt(long)]
    dataset: String,

    /// Number of iterations of each test run
    #[structopt(short = "i", long = "iterations", default_value = "3")]
    iterations: usize,

    /// Number of partitions to process in parallel. Defaults to number of available cores.
    #[structopt(short = "n", long = "partitions")]
    partitions: Option<usize>,

    /// Batch size when reading CSV or Parquet files
    #[structopt(short = "s", long = "batch-size")]
    batch_size: Option<usize>,

    /// Target input bytes accumulated by each cuDF aggregate chunk before flushing.
    #[structopt(long = "cudf-aggregate-chunk-target-bytes")]
    aggregate_chunk_target_bytes: Option<usize>,

    /// Target row count for batches uploaded to the GPU. Small upstream batches are
    /// coalesced per partition up to this size before pinning + upload.
    #[structopt(long)]
    cuda_batch_size: Option<usize>,

    /// Enable the experimental cuDF-backed Parquet scan.
    #[structopt(long = "enable-cudf-parquet-scan")]
    enable_cudf_parquet_scan: bool,

    /// Maximum number of files included in each cuDF Parquet read.
    #[structopt(long = "cudf-parquet-scan-files-per-batch")]
    cudf_parquet_scan_files_per_batch: Option<usize>,

    /// Activate debug mode to see more details
    #[structopt(short, long)]
    debug: bool,

    /// Run each query once before starting timed iterations.
    /// Useful to amortize one-shot costs (parquet metadata caching,
    /// CUDA context init, JIT, etc.) before measurement begins.
    #[structopt(long)]
    warmup: bool,

    /// Use the GPU (cuDF) execution path. When unset, runs on the CPU
    /// using DataFusion's default operators.
    #[structopt(long)]
    gpu: bool,

    /// Write per-query JSON results to this directory instead of the branch
    /// comparison store.
    #[structopt(long = "result-dir", parse(from_os_str), hidden = true)]
    result_dir: Option<PathBuf>,

    /// Skip comparing this run with the previous benchmark run.
    #[structopt(long = "no-compare", hidden = true)]
    no_compare: bool,

    /// Skip writing benchmark result JSON files.
    #[structopt(long = "no-store", hidden = true)]
    no_store: bool,
}

fn queries_for_dataset(dataset: &str) -> Result<Vec<(String, String)>, DataFusionError> {
    match dataset {
        "tpch" => tpch::get_queries()
            .into_iter()
            .map(|id| Ok((id.clone(), tpch::get_query(&id)?)))
            .collect(),
        "tpcds" => tpcds::get_queries()
            .into_iter()
            .filter(|id| id != "q72") // 72 is terribly slow
            .map(|id| Ok((id.clone(), tpcds::get_query(&id)?)))
            .collect(),
        "clickbench" => clickbench::get_queries()
            .into_iter()
            .map(|id| Ok((id.clone(), clickbench::get_query(&id)?)))
            .collect(),
        _ => not_impl_err!("Unknown benchmark dataset {dataset}"),
    }
}

impl RunOpt {
    fn config(&self) -> Result<SessionConfig> {
        let mut config = SessionConfig::from_env()?;
        if let Some(batch_size) = self.batch_size {
            config = config.with_batch_size(batch_size);
        }
        config = config.with_target_partitions(self.partitions());
        if self.gpu {
            let mut cudf_config = CuDFConfig::default()
                .with_enable(true)
                .with_parquet_scan(self.enable_cudf_parquet_scan);
            if let Some(bytes) = self.aggregate_chunk_target_bytes {
                cudf_config = cudf_config.with_aggregate_chunk_target_bytes(bytes);
            }
            if let Some(rows) = self.cuda_batch_size {
                cudf_config = cudf_config.with_batch_size(rows);
            }
            if let Some(files_per_batch) = self.cudf_parquet_scan_files_per_batch {
                cudf_config = cudf_config.with_parquet_scan_files_per_batch(files_per_batch);
            }
            config = config.with_option_extension(cudf_config);
        }
        Ok(config)
    }

    pub fn run(self) -> Result<()> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        rt.block_on(self.run_inner())
    }

    async fn run_inner(self) -> Result<()> {
        let benchmark_run = self.benchmark().await?;
        self.finish(benchmark_run)
    }

    fn finish(&self, benchmark_run: BenchmarkRun) -> Result<()> {
        if !self.no_compare {
            benchmark_run.compare_with_previous()?;
        }

        if self.no_store {
            return Ok(());
        }

        match &self.result_dir {
            Some(result_dir) => benchmark_run.store_results_to_dir(result_dir),
            None => benchmark_run.store(),
        }
    }

    async fn benchmark(&self) -> Result<BenchmarkRun> {
        let mut state_builder = SessionStateBuilder::new()
            .with_default_features()
            .with_config(self.config()?);
        if self.gpu {
            state_builder = state_builder.with_cudf_planner()
        }
        let state = state_builder.build();
        let ctx = SessionContext::new_with_state(state);

        if self.gpu {
            ctx.register_udaf((*avg()).clone());
            ctx.register_udaf((*count()).clone());
            ctx.register_udaf((*max()).clone());
            ctx.register_udaf((*min()).clone());
            ctx.register_udaf((*sum()).clone());
        }

        let dataset_prefix = self.dataset.split("_").next().unwrap();
        let queries = queries_for_dataset(dataset_prefix)?
            .into_iter()
            .filter(|(id, _)| self.query.is_empty() || self.query.contains(id))
            .collect::<Vec<_>>();
        for (_, sql) in &queries {
            apply_query_settings(&ctx, sql).await?;
        }
        register_tables(&ctx, &self.get_path()?).await?;

        println!("Running benchmarks with the following options: {self:?}");
        let mut benchmark_run = BenchmarkRun::new(self.dataset.clone(), self.gpu);

        for (id, sql) in queries {
            let query_id = format!("{} {id}", self.dataset);
            let query_run = self.benchmark_query(&query_id, &sql, &ctx).await;
            if let Err(e) = &query_run {
                eprintln!("{query_id} failed: {e:?}");
            }
            benchmark_run.results.push(query_run?);
        }

        Ok(benchmark_run)
    }

    async fn benchmark_query(
        &self,
        id: &str,
        sql: &str,
        ctx: &SessionContext,
    ) -> Result<BenchResult> {
        let mut bench_query = BenchResult {
            id: id.to_string(),
            dataset: self.dataset.clone(),
            iterations: vec![],
        };

        if self.warmup {
            for query in sql.split(";").map(|v| v.trim()) {
                if query.is_empty() {
                    continue;
                }
                match self.execute_query(ctx, query).await {
                    Ok(_) => println!("Query {id} warmup completed"),
                    Err(err) => println!("Query {id} warmup failed: {err}"),
                }
            }
        }

        'outer: for i in 0..self.iterations {
            let start = Instant::now();

            for query in sql.split(";").map(|v| v.trim()) {
                if query.starts_with("create") || query.starts_with("drop") {
                    self.execute_query(ctx, query).await?;
                    continue;
                } else if query.is_empty() {
                    continue;
                }

                match self.execute_query(ctx, query).await {
                    Ok(result) => {
                        let elapsed = start.elapsed();
                        let ms = elapsed.as_secs_f64() * 1000.0;
                        let row_count = result.iter().map(|b| b.num_rows()).sum();
                        println!(
                            "Query {id} iteration {i} took {ms:.1} ms and returned {row_count} rows"
                        );

                        bench_query.iterations.push(QueryIter {
                            elapsed,
                            row_count,
                            error: None,
                        });
                    }
                    Err(err) => {
                        println!("Query {id} iteration {i} failed: {err}");
                        bench_query.iterations.push(QueryIter {
                            elapsed: Duration::from_millis(0),
                            row_count: 0,
                            error: Some(err.to_string()),
                        });
                        continue 'outer;
                    }
                }
            }
        }
        println!("Query {id} avg time: {:.2} ms", bench_query.avg());

        Ok(bench_query)
    }

    async fn execute_query(&self, ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
        let plan = ctx.sql(sql).await?;
        let (state, plan) = plan.into_parts();

        let plan = state.optimize(&plan)?;
        let physical_plan = state.create_physical_plan(&plan).await?;
        let result = collect(physical_plan.clone(), state.task_ctx()).await;
        if self.debug {
            println!(
                "=== Physical plan with metrics ===\n{}\n",
                DisplayableExecutionPlan::with_metrics(physical_plan.as_ref()).indent(true)
            );
        }
        result
    }

    fn get_path(&self) -> Result<PathBuf> {
        let data_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("data")
            .join(&self.dataset);
        if !data_path.exists() {
            return exec_err!(
                "--dataset {} doesn't exist. Was it generated?",
                self.dataset
            );
        }

        let entries = fs::read_dir(&data_path)?.collect::<Result<Vec<_>, _>>()?;
        if entries.is_empty() {
            return exec_err!("Dataset {} is empty", self.dataset);
        }
        Ok(data_path)
    }

    fn partitions(&self) -> usize {
        self.partitions.unwrap_or_else(get_available_parallelism)
    }
}
