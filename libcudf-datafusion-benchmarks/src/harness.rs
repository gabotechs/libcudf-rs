use crate::results::BenchResult;
use datafusion::common::{exec_err, Result};
use serde::Serialize;
use std::cmp::Ordering;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime};
use structopt::StructOpt;

const DEFAULT_GPU_EXECUTION_BATCH_SIZE: usize = 65_536;

/// Run paired CPU/GPU benchmarks and write a reproducible report.
///
/// This is a thin harness around the existing `run` subcommand. It does not
/// compare query outputs; correctness is covered by tests.
#[derive(Debug, StructOpt, Clone)]
#[structopt(verbatim_doc_comment)]
pub struct HarnessOpt {
    /// Benchmark dataset under libcudf-datafusion-benchmarks/data
    #[structopt(long)]
    dataset: String,

    /// Query number(s). If not specified, runs all queries.
    #[structopt(short, long, use_delimiter = true)]
    query: Vec<String>,

    /// Number of iterations of each timed run.
    #[structopt(short = "i", long = "iterations", default_value = "3")]
    iterations: usize,

    /// Number of DataFusion execution partitions.
    #[structopt(short = "n", long = "partitions")]
    partitions: Option<usize>,

    /// DataFusion execution batch size for the CPU run.
    #[structopt(short = "s", long = "batch-size")]
    batch_size: Option<usize>,

    /// DataFusion execution batch size for the GPU run.
    #[structopt(long = "gpu-execution-batch-size")]
    gpu_execution_batch_size: Option<usize>,

    /// Target input bytes accumulated by each cuDF aggregate chunk before flushing.
    #[structopt(long = "cudf-aggregate-chunk-target-bytes")]
    aggregate_chunk_target_bytes: Option<usize>,

    /// Maximum RMM device-memory pool size for GPU runs.
    #[structopt(long = "cudf-device-pool-max-bytes")]
    device_pool_max_bytes: Option<usize>,

    /// Run each query once before timed iterations.
    #[structopt(long)]
    warmup: bool,

    /// Output root for benchmark run folders.
    ///
    /// Runs are written to `<output>/<dataset>/<run-id>`.
    #[structopt(
        parse(from_os_str),
        long = "output",
        default_value = concat!(env!("CARGO_MANIFEST_DIR"), "/benchmark-results")
    )]
    output: PathBuf,

    /// Stable run id. Defaults to `<unix-seconds>_<short-sha>`.
    #[structopt(long = "run-id")]
    run_id: Option<String>,

    /// Capture debug physical-plan logs for selected query number(s).
    #[structopt(long = "plan-query", use_delimiter = true)]
    plan_query: Vec<String>,

    /// Run `nsys profile` for selected GPU query number(s).
    #[structopt(long = "profile-query", use_delimiter = true)]
    profile_query: Vec<String>,
}

#[derive(Debug, Serialize)]
struct HarnessMetadata {
    run_id: String,
    dataset: String,
    queries: Vec<String>,
    iterations: usize,
    partitions: Option<usize>,
    cpu_execution_batch_size: Option<usize>,
    gpu_execution_batch_size: Option<usize>,
    device_pool_max_bytes: Option<usize>,
    aggregate_chunk_target_bytes: Option<usize>,
    warmup: bool,
    plan_queries: Vec<String>,
    profile_queries: Vec<String>,
    started_at_unix: u64,
    finished_at_unix: u64,
    git_branch: String,
    git_sha: String,
    git_dirty: bool,
    cpu_cores: usize,
    gpu: Option<String>,
    cpu_command: String,
    gpu_command: String,
}

#[derive(Debug)]
struct RunStats {
    avg_ms: Option<f64>,
    median_ms: Option<f64>,
    min_ms: Option<f64>,
    max_ms: Option<f64>,
    row_count: Option<usize>,
    error: Option<String>,
    successful_iterations: usize,
    total_iterations: usize,
}

impl RunStats {
    fn is_complete(&self) -> bool {
        self.error.is_none()
            && self.total_iterations > 0
            && self.successful_iterations == self.total_iterations
    }
}

impl HarnessOpt {
    pub fn run(self) -> Result<()> {
        let started_at_unix = unix_seconds();
        let branch = current_branch();
        let run_id = self
            .run_id
            .clone()
            .unwrap_or_else(|| format!("{}_{}", started_at_unix, short_sha()));
        let run_dir = self
            .output
            .join(dataset_path_component(&self.dataset))
            .join(&run_id);
        let logs_dir = run_dir.join("logs");
        fs::create_dir_all(&logs_dir)?;

        let exe = std::env::current_exe()?;

        let cpu_dir = run_dir.join("cpu");
        let gpu_dir = run_dir.join("gpu");
        reset_dir(&cpu_dir)?;
        reset_dir(&gpu_dir)?;

        let cpu_execution_batch_size = self.cpu_execution_batch_size();
        let gpu_execution_batch_size = self.gpu_execution_batch_size();
        let cpu_args = self.dfbench_run_args(
            false,
            cpu_execution_batch_size,
            false,
            None,
            self.iterations,
            Some(&cpu_dir),
            true,
        );
        let gpu_args = self.dfbench_run_args(
            true,
            gpu_execution_batch_size,
            false,
            None,
            self.iterations,
            Some(&gpu_dir),
            true,
        );

        run_logged(&exe, &cpu_args, &logs_dir.join("cpu.log"))?;

        run_logged(&exe, &gpu_args, &logs_dir.join("gpu.log"))?;

        self.capture_plan_logs(&exe, &run_dir)?;
        self.capture_profiles(&exe, &run_dir)?;

        let metadata = HarnessMetadata {
            run_id,
            dataset: self.dataset.clone(),
            queries: self.query.clone(),
            iterations: self.iterations,
            partitions: self.partitions,
            cpu_execution_batch_size,
            gpu_execution_batch_size,
            device_pool_max_bytes: self.device_pool_max_bytes,
            aggregate_chunk_target_bytes: self.aggregate_chunk_target_bytes,
            warmup: self.warmup,
            plan_queries: self.plan_query.clone(),
            profile_queries: self.profile_query.clone(),
            started_at_unix,
            finished_at_unix: unix_seconds(),
            git_branch: branch,
            git_sha: git_output(["rev-parse", "HEAD"]).unwrap_or_default(),
            git_dirty: git_dirty(),
            cpu_cores: std::thread::available_parallelism().map_or(1, usize::from),
            gpu: command_output(
                "nvidia-smi",
                [
                    "--query-gpu=name,memory.total,driver_version",
                    "--format=csv,noheader",
                ],
            ),
            cpu_command: command_string(&exe, &cpu_args),
            gpu_command: command_string(&exe, &gpu_args),
        };

        let metadata_json = serde_json::to_string_pretty(&metadata).unwrap();
        fs::write(run_dir.join("metadata.json"), metadata_json)?;
        write_report(&run_dir, &metadata)?;

        println!(
            "Benchmark report written to {}",
            run_dir.join("report.md").display()
        );
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn dfbench_run_args(
        &self,
        gpu: bool,
        batch_size: Option<usize>,
        debug: bool,
        query: Option<&str>,
        iterations: usize,
        result_dir: Option<&Path>,
        store_results: bool,
    ) -> Vec<String> {
        let mut args = vec![
            "run".to_string(),
            "--dataset".to_string(),
            self.dataset.clone(),
            "--iterations".to_string(),
            iterations.to_string(),
        ];

        if let Some(partitions) = self.partitions {
            args.push("--partitions".to_string());
            args.push(partitions.to_string());
        }
        if let Some(batch_size) = batch_size {
            args.push("--batch-size".to_string());
            args.push(batch_size.to_string());
        }
        if self.warmup {
            args.push("--warmup".to_string());
        }
        if gpu {
            args.push("--gpu".to_string());
            if let Some(bytes) = self.device_pool_max_bytes {
                args.push("--cudf-device-pool-max-bytes".to_string());
                args.push(bytes.to_string());
            }
            if let Some(bytes) = self.aggregate_chunk_target_bytes {
                args.push("--cudf-aggregate-chunk-target-bytes".to_string());
                args.push(bytes.to_string());
            }
        }
        if debug {
            args.push("--debug".to_string());
        }
        if let Some(result_dir) = result_dir {
            args.push("--result-dir".to_string());
            args.push(result_dir.display().to_string());
        }
        args.push("--no-compare".to_string());
        if !store_results {
            args.push("--no-store".to_string());
        }

        match query {
            Some(query) => {
                args.push("--query".to_string());
                args.push(query.to_string());
            }
            None if !self.query.is_empty() => {
                args.push("--query".to_string());
                args.push(self.query.join(","));
            }
            None => {}
        }

        args
    }

    fn cpu_execution_batch_size(&self) -> Option<usize> {
        self.batch_size
    }

    fn gpu_execution_batch_size(&self) -> Option<usize> {
        Some(
            self.gpu_execution_batch_size
                .unwrap_or(DEFAULT_GPU_EXECUTION_BATCH_SIZE),
        )
    }

    fn capture_plan_logs(&self, exe: &Path, run_dir: &Path) -> Result<()> {
        if self.plan_query.is_empty() {
            return Ok(());
        }

        let plans_dir = run_dir.join("plans");
        fs::create_dir_all(&plans_dir)?;
        for query in &self.plan_query {
            let cpu_args = self.dfbench_run_args(
                false,
                self.cpu_execution_batch_size(),
                true,
                Some(query),
                1,
                None,
                false,
            );
            run_logged(
                exe,
                &cpu_args,
                &plans_dir.join(format!("{query}_cpu_debug.log")),
            )?;

            let gpu_args = self.dfbench_run_args(
                true,
                self.gpu_execution_batch_size(),
                true,
                Some(query),
                1,
                None,
                false,
            );
            run_logged(
                exe,
                &gpu_args,
                &plans_dir.join(format!("{query}_gpu_debug.log")),
            )?;
        }
        Ok(())
    }

    fn capture_profiles(&self, exe: &Path, run_dir: &Path) -> Result<()> {
        if self.profile_query.is_empty() {
            return Ok(());
        }
        if command_output("which", ["nsys"]).is_none() {
            return exec_err!("--profile-query requested but `nsys` is not available");
        }

        let profiles_dir = run_dir.join("profiles");
        fs::create_dir_all(&profiles_dir)?;
        for query in &self.profile_query {
            let profile_base = profiles_dir.join(format!("{query}_gpu_nsys"));
            let mut args = vec![
                "profile".to_string(),
                "--force-overwrite=true".to_string(),
                "--trace=cuda,nvtx,osrt".to_string(),
                "--stats=true".to_string(),
                "-o".to_string(),
                profile_base.display().to_string(),
                exe.display().to_string(),
            ];
            args.extend(self.dfbench_run_args(
                true,
                self.gpu_execution_batch_size(),
                false,
                Some(query),
                1,
                None,
                false,
            ));
            run_logged(
                Path::new("nsys"),
                &args,
                &profiles_dir.join(format!("{query}_gpu_nsys.log")),
            )?;
        }
        Ok(())
    }
}

fn run_logged(program: &Path, args: &[String], log_path: &Path) -> Result<()> {
    if let Some(parent) = log_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let output = Command::new(program).args(args).output()?;
    let mut log = Vec::new();
    log.extend_from_slice(command_string(program, args).as_bytes());
    log.extend_from_slice(b"\n\n--- stdout ---\n");
    log.extend_from_slice(&output.stdout);
    log.extend_from_slice(b"\n--- stderr ---\n");
    log.extend_from_slice(&output.stderr);
    fs::write(log_path, log)?;

    if !output.status.success() {
        return exec_err!(
            "benchmark command failed with status {:?}; see {}",
            output.status.code(),
            log_path.display()
        );
    }
    Ok(())
}

fn reset_dir(path: &Path) -> Result<()> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    fs::create_dir_all(path)?;
    Ok(())
}

fn write_report(run_dir: &Path, metadata: &HarnessMetadata) -> Result<()> {
    let cpu = load_results(&run_dir.join("cpu"))?;
    let gpu = load_results(&run_dir.join("gpu"))?;

    let mut report = String::new();
    report.push_str("# Benchmark Report\n\n");
    report.push_str(
        "This harness does not compare query outputs. Correctness is covered by tests.\n\n",
    );
    report.push_str("## Metadata\n\n");
    report.push_str(&format!("- run id: `{}`\n", metadata.run_id));
    report.push_str(&format!("- dataset: `{}`\n", metadata.dataset));
    report.push_str(&format!("- git sha: `{}`\n", metadata.git_sha));
    report.push_str(&format!("- git branch: `{}`\n", metadata.git_branch));
    report.push_str(&format!("- dirty worktree: `{}`\n", metadata.git_dirty));
    report.push_str(&format!("- cpu cores: `{}`\n", metadata.cpu_cores));
    if let Some(gpu) = &metadata.gpu {
        report.push_str(&format!("- gpu: `{gpu}`\n"));
    }
    report.push_str(&format!("- iterations: `{}`\n", metadata.iterations));
    report.push_str(&format!("- partitions: `{:?}`\n", metadata.partitions));
    report.push_str(&format!(
        "- cpu execution batch size: `{:?}`\n",
        metadata.cpu_execution_batch_size
    ));
    report.push_str(&format!(
        "- gpu execution batch size: `{:?}`\n",
        metadata.gpu_execution_batch_size
    ));
    report.push_str(&format!(
        "- cuDF device pool max bytes: `{:?}`\n",
        metadata.device_pool_max_bytes
    ));
    report.push_str(&format!(
        "- cuDF aggregate chunk target bytes: `{:?}`\n",
        metadata.aggregate_chunk_target_bytes
    ));
    report.push_str(&format!("- warmup: `{}`\n\n", metadata.warmup));

    report.push_str("## Commands\n\n");
    report.push_str(&format!(
        "```text\n{}\n{}\n```\n\n",
        metadata.cpu_command, metadata.gpu_command
    ));

    report.push_str("## Artifacts\n\n");
    report.push_str(&format!(
        "- CPU result JSONs: {}\n",
        artifact_paths("cpu", &cpu)
    ));
    report.push_str(&format!(
        "- GPU result JSONs: {}\n",
        artifact_paths("gpu", &gpu)
    ));
    report.push_str("- CPU run log: `logs/cpu.log`\n");
    report.push_str("- GPU run log: `logs/gpu.log`\n");
    report.push_str("- Run metadata: `metadata.json`\n");
    if !metadata.plan_queries.is_empty() {
        report.push_str("- Debug plans: `plans/<query>_{cpu,gpu}_debug.log`\n");
    }
    if !metadata.profile_queries.is_empty() {
        report.push_str("- Nsight profiles: `profiles/<query>_gpu_nsys.*`\n");
    }
    report.push('\n');

    report.push_str("## Results\n\n");
    report.push_str("| Query | CPU avg/med/min/max ms | GPU avg/med/min/max ms | GPU speedup | CPU rows | GPU rows | Status |\n");
    report.push_str("|---:|---:|---:|---:|---:|---:|---|\n");

    let mut total_cpu = 0.0;
    let mut total_gpu = 0.0;
    let mut ratios = Vec::new();
    let mut paired = 0usize;

    for cpu_result in &cpu {
        let Some(gpu_result) = gpu.iter().find(|v| v.id == cpu_result.id) else {
            let cpu_stats = stats(cpu_result);
            report.push_str(&format!(
                "| {} | {} | missing | n/a | {} | n/a | missing gpu |\n",
                query_label(&cpu_result.id),
                format_stats(&cpu_stats),
                row_count(&cpu_stats),
            ));
            continue;
        };

        let cpu_stats = stats(cpu_result);
        let gpu_stats = stats(gpu_result);
        let status = status(&cpu_stats, &gpu_stats);
        let speedup = match (&cpu_stats, &gpu_stats) {
            (cpu, gpu) if cpu.is_complete() && gpu.is_complete() => {
                let cpu_avg = cpu.avg_ms.unwrap();
                let gpu_avg = gpu.avg_ms.unwrap();
                total_cpu += cpu_avg;
                total_gpu += gpu_avg;
                ratios.push(cpu_avg / gpu_avg);
                paired += 1;
                format!("{:.2}x", cpu_avg / gpu_avg)
            }
            _ => "n/a".to_string(),
        };

        report.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} |\n",
            query_label(&cpu_result.id),
            format_stats(&cpu_stats),
            format_stats(&gpu_stats),
            speedup,
            row_count(&cpu_stats),
            row_count(&gpu_stats),
            status,
        ));
    }

    if paired > 0 {
        let geo_mean = ratios
            .iter()
            .product::<f64>()
            .powf(1.0 / ratios.len() as f64);
        report.push_str("\n## Summary\n\n");
        report.push_str(&format!("- paired successful queries: `{paired}`\n"));
        report.push_str(&format!("- CPU total avg: `{:.1} ms`\n", total_cpu));
        report.push_str(&format!("- GPU total avg: `{:.1} ms`\n", total_gpu));
        report.push_str(&format!(
            "- GPU speedup by total average: `{:.2}x`\n",
            total_cpu / total_gpu
        ));
        report.push_str(&format!("- geometric mean speedup: `{geo_mean:.2}x`\n"));
    }

    fs::write(run_dir.join("report.md"), report)?;
    Ok(())
}

fn load_results(dir: &Path) -> Result<Vec<BenchResult>> {
    let mut results = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|v| v.to_str()) != Some("json") {
            continue;
        }
        let bytes = fs::read(&path)?;
        let result: BenchResult = serde_json::from_slice(&bytes)
            .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))?;
        results.push(result);
    }
    results.sort_by_key(|r| query_sort_key(&r.id));
    Ok(results)
}

fn stats(result: &BenchResult) -> RunStats {
    let error = result.iterations.iter().find_map(|v| v.error.clone());
    let mut values: Vec<f64> = result
        .iterations
        .iter()
        .filter(|v| v.error.is_none())
        .map(|v| duration_ms(v.elapsed))
        .collect();
    let row_count = result
        .iterations
        .iter()
        .find(|v| v.error.is_none())
        .map(|v| v.row_count);
    let total_iterations = result.iterations.len();
    let successful_iterations = values.len();

    if values.is_empty() {
        return RunStats {
            avg_ms: None,
            median_ms: None,
            min_ms: None,
            max_ms: None,
            row_count,
            error,
            successful_iterations,
            total_iterations,
        };
    }

    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let avg_ms = values.iter().sum::<f64>() / values.len() as f64;
    let mid = values.len() / 2;
    let median_ms = if values.len().is_multiple_of(2) {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    };

    RunStats {
        avg_ms: Some(avg_ms),
        median_ms: Some(median_ms),
        min_ms: values.first().copied(),
        max_ms: values.last().copied(),
        row_count,
        error,
        successful_iterations,
        total_iterations,
    }
}

fn format_stats(stats: &RunStats) -> String {
    match (stats.avg_ms, stats.median_ms, stats.min_ms, stats.max_ms) {
        (Some(avg), Some(median), Some(min), Some(max)) => {
            format!("{:.1}/{:.1}/{:.1}/{:.1}", avg, median, min, max)
        }
        _ => "n/a".to_string(),
    }
}

fn row_count(stats: &RunStats) -> String {
    stats
        .row_count
        .map(|v| v.to_string())
        .unwrap_or_else(|| "n/a".to_string())
}

fn artifact_paths(prefix: &str, results: &[BenchResult]) -> String {
    if results.is_empty() {
        return "`none`".to_string();
    }

    let paths = results
        .iter()
        .map(|result| format!("`{prefix}/{}.json`", query_label(&result.id)))
        .collect::<Vec<_>>();

    if paths.len() <= 8 {
        return paths.join(", ");
    }

    let remaining = paths.len() - 8;
    format!("{}, and {remaining} more", paths[..8].join(", "))
}

fn status(cpu: &RunStats, gpu: &RunStats) -> String {
    let mut states = Vec::new();

    if let Some(state) = incomplete_state("cpu", cpu) {
        states.push(state);
    }
    if let Some(state) = incomplete_state("gpu", gpu) {
        states.push(state);
    }

    if states.is_empty() {
        "ok".to_string()
    } else {
        states.join(", ")
    }
}

fn incomplete_state(label: &str, stats: &RunStats) -> Option<String> {
    if stats.is_complete() {
        return None;
    }

    let state = if stats.successful_iterations == 0 {
        "error"
    } else if stats.error.is_some() {
        "partial"
    } else {
        "empty"
    };

    if state == "empty" {
        Some(format!("{label} empty"))
    } else if stats.total_iterations > 0 {
        Some(format!(
            "{label} {state} ({}/{})",
            stats.successful_iterations, stats.total_iterations
        ))
    } else {
        Some(format!("{label} {state}"))
    }
}

fn query_label(id: &str) -> String {
    id.rsplit_once(' ').map_or(id, |(_, q)| q).to_string()
}

fn query_sort_key(id: &str) -> (u32, String) {
    let label = query_label(id);
    let number = label
        .strip_prefix('q')
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(u32::MAX);
    (number, label)
}

fn dataset_path_component(dataset: &str) -> String {
    dataset.replace(['/', '\\'], "_")
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn command_string(program: &Path, args: &[String]) -> String {
    std::iter::once(program.display().to_string())
        .chain(args.iter().cloned())
        .collect::<Vec<_>>()
        .join(" ")
}

fn command_output<const N: usize>(program: &str, args: [&str; N]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn git_output<const N: usize>(args: [&str; N]) -> Option<String> {
    command_output("git", args)
}

fn current_branch() -> String {
    git_output(["rev-parse", "--abbrev-ref", "HEAD"])
        .unwrap_or_else(|| "unknown".to_string())
        .split('/')
        .next_back()
        .unwrap_or("unknown")
        .to_string()
}

fn short_sha() -> String {
    git_output(["rev-parse", "--short", "HEAD"]).unwrap_or_else(|| "unknown".to_string())
}

fn git_dirty() -> bool {
    git_output(["status", "--porcelain"])
        .map(|status| !status.is_empty())
        .unwrap_or(false)
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("current time is later than UNIX_EPOCH")
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::results::QueryIter;

    #[test]
    fn partial_iteration_stats_are_incomplete(
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let result = BenchResult {
            id: "tpch_sf1 q1".to_string(),
            dataset: "tpch_sf1".to_string(),
            iterations: vec![
                QueryIter {
                    row_count: 1,
                    elapsed: Duration::from_millis(10),
                    error: None,
                },
                QueryIter {
                    row_count: 0,
                    elapsed: Duration::ZERO,
                    error: Some("failed".to_string()),
                },
            ],
        };

        let stats = stats(&result);

        assert_eq!(stats.successful_iterations, 1);
        assert_eq!(stats.total_iterations, 2);
        assert!(!stats.is_complete());
        assert_eq!(format_stats(&stats), "10.0/10.0/10.0/10.0");
        Ok(())
    }

    #[test]
    fn dataset_path_component_replaces_path_separators() {
        assert_eq!(dataset_path_component("tpch_sf1"), "tpch_sf1");
        assert_eq!(dataset_path_component("nested/tpch"), "nested_tpch");
        assert_eq!(dataset_path_component(r"nested\tpch"), "nested_tpch");
    }

    #[test]
    fn run_args_use_gpu_execution_batch_size_override() {
        let opt = harness_opt(Some(8192), Some(131072));

        let cpu_args = opt.dfbench_run_args(
            false,
            opt.cpu_execution_batch_size(),
            false,
            None,
            3,
            Some(Path::new("/tmp/cpu")),
            true,
        );
        let gpu_args = opt.dfbench_run_args(
            true,
            opt.gpu_execution_batch_size(),
            false,
            None,
            3,
            Some(Path::new("/tmp/gpu")),
            true,
        );

        assert!(contains_arg_pair(&cpu_args, "--batch-size", "8192"));
        assert!(contains_arg_pair(&gpu_args, "--batch-size", "131072"));

        let default_opt = harness_opt(Some(8192), None);
        let default_gpu_args = default_opt.dfbench_run_args(
            true,
            default_opt.gpu_execution_batch_size(),
            false,
            None,
            3,
            Some(Path::new("/tmp/gpu")),
            true,
        );
        assert!(contains_arg_pair(
            &default_gpu_args,
            "--batch-size",
            "65536"
        ));

        let no_batch_opt = harness_opt(None, None);
        let cpu_args = no_batch_opt.dfbench_run_args(
            false,
            no_batch_opt.cpu_execution_batch_size(),
            false,
            None,
            3,
            Some(Path::new("/tmp/cpu")),
            true,
        );
        let gpu_args = no_batch_opt.dfbench_run_args(
            true,
            no_batch_opt.gpu_execution_batch_size(),
            false,
            None,
            3,
            Some(Path::new("/tmp/gpu")),
            true,
        );
        assert!(!cpu_args.iter().any(|arg| arg == "--batch-size"));
        assert!(contains_arg_pair(&gpu_args, "--batch-size", "65536"));
    }

    fn harness_opt(
        batch_size: Option<usize>,
        gpu_execution_batch_size: Option<usize>,
    ) -> HarnessOpt {
        HarnessOpt {
            dataset: "tpch_sf1".to_string(),
            query: Vec::new(),
            iterations: 3,
            partitions: Some(4),
            batch_size,
            gpu_execution_batch_size,
            device_pool_max_bytes: None,
            aggregate_chunk_target_bytes: None,
            warmup: false,
            output: PathBuf::from("/tmp/bench-results"),
            run_id: None,
            plan_query: Vec::new(),
            profile_query: Vec::new(),
        }
    }

    fn contains_arg_pair(args: &[String], key: &str, value: &str) -> bool {
        args.windows(2)
            .any(|pair| pair[0] == key && pair[1] == value)
    }
}
