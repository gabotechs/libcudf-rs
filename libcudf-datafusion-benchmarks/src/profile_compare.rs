use datafusion::common::{exec_err, Result};
use rusqlite::Connection;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use structopt::StructOpt;

/// Compare Nsight profile artifacts from two benchmark harness runs.
#[derive(Debug, StructOpt, Clone)]
#[structopt(verbatim_doc_comment)]
pub struct ProfileCompareOpt {
    /// Benchmark dataset under libcudf-datafusion-benchmarks/benchmark-results.
    #[structopt(long)]
    dataset: String,

    /// Baseline harness run id.
    #[structopt(long = "baseline-run-id")]
    baseline_run_id: String,

    /// Candidate harness run id.
    #[structopt(long = "candidate-run-id")]
    candidate_run_id: String,

    /// Query number(s). If not specified, compares all common profiles.
    #[structopt(short, long, use_delimiter = true)]
    query: Vec<String>,

    /// Output root for benchmark run folders.
    #[structopt(
        parse(from_os_str),
        long = "output",
        default_value = concat!(env!("CARGO_MANIFEST_DIR"), "/benchmark-results")
    )]
    output: PathBuf,

    /// Number of rows to include in runtime and hotspot sections.
    #[structopt(long = "top", default_value = "5")]
    top: usize,
}

#[derive(Debug, Serialize)]
struct ProfileComparison {
    dataset: String,
    baseline_run_id: String,
    candidate_run_id: String,
    compared_queries: Vec<String>,
    baseline_dir: String,
    candidate_dir: String,
    baseline_totals: ProfileCategories,
    candidate_totals: ProfileCategories,
    queries: Vec<QueryComparison>,
}

#[derive(Debug, Serialize)]
struct QueryComparison {
    query: String,
    baseline: ProfileSummary,
    candidate: ProfileSummary,
    top_runtime_changes: Vec<RuntimeChange>,
}

#[derive(Debug, Serialize, Clone, Default)]
struct ProfileSummary {
    query: String,
    sqlite_path: String,
    categories: ProfileCategories,
    top_runtime_api: Vec<NamedStat>,
    memcpy_by_kind: Vec<NamedStat>,
    kernel_groups: Vec<KernelGroup>,
    #[serde(skip)]
    runtime_api_by_name: BTreeMap<String, NamedStat>,
}

#[derive(Debug, Serialize, Clone, Default)]
struct ProfileCategories {
    runtime_api: Stat,
    device_alloc_api: Stat,
    host_alloc_api: Stat,
    memcpy_api: Stat,
    sync_api: Stat,
    kernel_launch_api: Stat,
    memcpy_activity: Stat,
    kernel_activity: Stat,
}

#[derive(Debug, Serialize, Clone, Default)]
struct Stat {
    count: i64,
    time_ms: f64,
    bytes: i64,
}

#[derive(Debug, Serialize, Clone, Default)]
struct NamedStat {
    name: String,
    count: i64,
    time_ms: f64,
    bytes: i64,
}

#[derive(Debug, Serialize, Clone, Default)]
struct KernelGroup {
    name: String,
    count: i64,
    time_ms: f64,
    unique_full_names: i64,
}

#[derive(Debug, Serialize)]
struct RuntimeChange {
    name: String,
    baseline: Stat,
    candidate: Stat,
    delta_count: i64,
    delta_time_ms: f64,
}

impl ProfileCompareOpt {
    pub fn run(&self) -> Result<()> {
        if self.top == 0 {
            return exec_err!("--top must be greater than 0");
        }

        let dataset_dir = self.output.join(path_component(&self.dataset));
        let baseline_dir = dataset_dir.join(&self.baseline_run_id);
        let candidate_dir = dataset_dir.join(&self.candidate_run_id);

        let baseline_profiles = load_profiles(&baseline_dir, &self.query, self.top)?;
        let candidate_profiles = load_profiles(&candidate_dir, &self.query, self.top)?;
        let queries = common_queries(&baseline_profiles, &candidate_profiles);

        if queries.is_empty() {
            return exec_err!(
                "no common Nsight profiles found under {} and {}",
                baseline_dir.join("profiles").display(),
                candidate_dir.join("profiles").display()
            );
        }

        let baseline_totals = totals(queries.iter().map(|query| &baseline_profiles[query]));
        let candidate_totals = totals(queries.iter().map(|query| &candidate_profiles[query]));
        let query_comparisons = queries
            .iter()
            .map(|query| {
                let baseline = baseline_profiles[query].clone();
                let candidate = candidate_profiles[query].clone();
                let top_runtime_changes = runtime_changes(&baseline, &candidate, self.top);
                QueryComparison {
                    query: query.clone(),
                    baseline,
                    candidate,
                    top_runtime_changes,
                }
            })
            .collect::<Vec<_>>();

        let comparison = ProfileComparison {
            dataset: self.dataset.clone(),
            baseline_run_id: self.baseline_run_id.clone(),
            candidate_run_id: self.candidate_run_id.clone(),
            compared_queries: queries,
            baseline_dir: baseline_dir.display().to_string(),
            candidate_dir: candidate_dir.display().to_string(),
            baseline_totals,
            candidate_totals,
            queries: query_comparisons,
        };

        let comparison_dir = dataset_dir.join("comparisons").join(format!(
            "{}__{}",
            path_component(&self.baseline_run_id),
            path_component(&self.candidate_run_id)
        ));
        fs::create_dir_all(&comparison_dir)?;
        fs::write(
            comparison_dir.join("profile-compare.json"),
            serde_json::to_string_pretty(&comparison).unwrap(),
        )?;
        fs::write(
            comparison_dir.join("profile-compare.md"),
            render_markdown(&comparison, self.top),
        )?;

        println!(
            "Profile comparison written to {}",
            comparison_dir.join("profile-compare.md").display()
        );
        Ok(())
    }
}

fn load_profiles(
    run_dir: &Path,
    query_filter: &[String],
    top: usize,
) -> Result<BTreeMap<String, ProfileSummary>> {
    let profiles_dir = run_dir.join("profiles");
    if !profiles_dir.exists() {
        return exec_err!(
            "profile directory does not exist: {}",
            profiles_dir.display()
        );
    }

    let query_filter = query_filter.iter().cloned().collect::<BTreeSet<_>>();
    let mut profiles = BTreeMap::new();
    for entry in fs::read_dir(&profiles_dir)? {
        let path = entry?.path();
        if path.extension().and_then(|value| value.to_str()) != Some("sqlite") {
            continue;
        }

        let Some(query) = path
            .file_name()
            .and_then(|value| value.to_str())
            .and_then(|value| value.strip_suffix("_gpu_nsys.sqlite"))
        else {
            continue;
        };
        if !query_filter.is_empty() && !query_filter.contains(query) {
            continue;
        }

        profiles.insert(query.to_string(), load_profile(query, &path, top)?);
    }

    if !query_filter.is_empty() {
        let loaded = profiles.keys().cloned().collect::<BTreeSet<_>>();
        let missing = query_filter
            .difference(&loaded)
            .cloned()
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return exec_err!(
                "missing profile(s) under {}: {}",
                profiles_dir.display(),
                missing.join(", ")
            );
        }
    }

    Ok(profiles)
}

fn load_profile(query: &str, path: &Path, top: usize) -> Result<ProfileSummary> {
    let conn = sql(Connection::open(path))?;
    let runtime_api_by_name = runtime_api_by_name(&conn)?;
    let mut top_runtime_api = runtime_api_by_name.values().cloned().collect::<Vec<_>>();
    top_runtime_api.sort_by(|left, right| {
        right
            .time_ms
            .partial_cmp(&left.time_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    top_runtime_api.truncate(top);

    Ok(ProfileSummary {
        query: query.to_string(),
        sqlite_path: path.display().to_string(),
        categories: ProfileCategories {
            runtime_api: runtime_stats(&conn, "1 = 1")?,
            device_alloc_api: runtime_stats(
                &conn,
                "s.value GLOB 'cudaMalloc_v*' OR s.value GLOB 'cudaMallocAsync_v*' OR \
                 s.value GLOB 'cudaFree_v*' OR s.value GLOB 'cudaFreeAsync_v*'",
            )?,
            host_alloc_api: runtime_stats(
                &conn,
                "s.value GLOB 'cudaHostAlloc*' OR s.value GLOB 'cudaMallocHost*' OR \
                 s.value GLOB 'cudaFreeHost*'",
            )?,
            memcpy_api: runtime_stats(&conn, "s.value GLOB 'cudaMemcpy*'")?,
            sync_api: runtime_stats(&conn, "s.value GLOB 'cuda*Synchronize*'")?,
            kernel_launch_api: runtime_stats(&conn, "s.value GLOB 'cudaLaunchKernel*'")?,
            memcpy_activity: memcpy_total(&conn)?,
            kernel_activity: kernel_total(&conn)?,
        },
        top_runtime_api,
        memcpy_by_kind: memcpy_by_kind(&conn)?,
        kernel_groups: kernel_groups(&conn, top)?,
        runtime_api_by_name,
    })
}

fn runtime_stats(conn: &Connection, predicate: &str) -> Result<Stat> {
    if !has_runtime_tables(conn)? {
        return Ok(Stat::default());
    }

    sql(conn.query_row(
        &format!(
            "select count(*), coalesce(sum(r.end - r.start), 0) / 1000000.0 \
             from CUPTI_ACTIVITY_KIND_RUNTIME r \
             join StringIds s on s.id = r.nameId \
             where {predicate}"
        ),
        [],
        |row| {
            Ok(Stat {
                count: row.get(0)?,
                time_ms: row.get(1)?,
                bytes: 0,
            })
        },
    ))
}

fn runtime_api_by_name(conn: &Connection) -> Result<BTreeMap<String, NamedStat>> {
    if !has_runtime_tables(conn)? {
        return Ok(BTreeMap::new());
    }

    let stats = named_stats(
        conn,
        "select s.value, count(*), coalesce(sum(r.end - r.start), 0) / 1000000.0, 0 \
         from CUPTI_ACTIVITY_KIND_RUNTIME r \
         join StringIds s on s.id = r.nameId \
         group by s.value \
         order by coalesce(sum(r.end - r.start), 0) desc",
    )?;
    Ok(stats
        .into_iter()
        .map(|stat| (stat.name.clone(), stat))
        .collect())
}

fn memcpy_total(conn: &Connection) -> Result<Stat> {
    if !table_exists(conn, "CUPTI_ACTIVITY_KIND_MEMCPY")? {
        return Ok(Stat::default());
    }

    sql(conn.query_row(
        "select count(*), coalesce(sum(end - start), 0) / 1000000.0, coalesce(sum(bytes), 0) \
         from CUPTI_ACTIVITY_KIND_MEMCPY",
        [],
        |row| {
            Ok(Stat {
                count: row.get(0)?,
                time_ms: row.get(1)?,
                bytes: row.get(2)?,
            })
        },
    ))
}

fn memcpy_by_kind(conn: &Connection) -> Result<Vec<NamedStat>> {
    if !table_exists(conn, "CUPTI_ACTIVITY_KIND_MEMCPY")? {
        return Ok(Vec::new());
    }

    let query = if table_exists(conn, "ENUM_CUDA_MEMCPY_OPER")? {
        "select coalesce(o.label, 'kind ' || m.copyKind), count(*), \
         coalesce(sum(m.end - m.start), 0) / 1000000.0, coalesce(sum(m.bytes), 0) \
         from CUPTI_ACTIVITY_KIND_MEMCPY m \
         left join ENUM_CUDA_MEMCPY_OPER o on o.id = m.copyKind \
         group by m.copyKind, o.label \
         order by coalesce(sum(m.end - m.start), 0) desc"
    } else {
        "select 'kind ' || m.copyKind, count(*), \
         coalesce(sum(m.end - m.start), 0) / 1000000.0, coalesce(sum(m.bytes), 0) \
         from CUPTI_ACTIVITY_KIND_MEMCPY m \
         group by m.copyKind \
         order by coalesce(sum(m.end - m.start), 0) desc"
    };
    named_stats(conn, query)
}

fn kernel_total(conn: &Connection) -> Result<Stat> {
    if !table_exists(conn, "CUPTI_ACTIVITY_KIND_KERNEL")? {
        return Ok(Stat::default());
    }

    sql(conn.query_row(
        "select count(*), coalesce(sum(end - start), 0) / 1000000.0 \
         from CUPTI_ACTIVITY_KIND_KERNEL",
        [],
        |row| {
            Ok(Stat {
                count: row.get(0)?,
                time_ms: row.get(1)?,
                bytes: 0,
            })
        },
    ))
}

fn kernel_groups(conn: &Connection, top: usize) -> Result<Vec<KernelGroup>> {
    if !table_exists(conn, "CUPTI_ACTIVITY_KIND_KERNEL")? || !table_exists(conn, "StringIds")? {
        return Ok(Vec::new());
    }

    let mut stmt = sql(conn.prepare(
        "select coalesce(short.value, d.value, mangled.value, '<unknown>'), count(*), \
         coalesce(sum(k.end - k.start), 0) / 1000000.0, \
         count(distinct coalesce(d.value, short.value, mangled.value, '<unknown>')) \
         from CUPTI_ACTIVITY_KIND_KERNEL k \
         left join StringIds d on d.id = k.demangledName \
         left join StringIds short on short.id = k.shortName \
         left join StringIds mangled on mangled.id = k.mangledName \
         group by 1 \
         order by coalesce(sum(k.end - k.start), 0) desc",
    ))?;
    let rows = sql(stmt.query_map([], |row| {
        Ok(KernelGroup {
            name: row.get(0)?,
            count: row.get(1)?,
            time_ms: row.get(2)?,
            unique_full_names: row.get(3)?,
        })
    }))?;

    let mut groups = Vec::new();
    for row in rows {
        groups.push(sql(row)?);
        if groups.len() >= top {
            break;
        }
    }
    Ok(groups)
}

fn named_stats(conn: &Connection, query: &str) -> Result<Vec<NamedStat>> {
    let mut stmt = sql(conn.prepare(query))?;
    let rows = sql(stmt.query_map([], |row| {
        Ok(NamedStat {
            name: row.get(0)?,
            count: row.get(1)?,
            time_ms: row.get(2)?,
            bytes: row.get(3)?,
        })
    }))?;

    let mut stats = Vec::new();
    for row in rows {
        stats.push(sql(row)?);
    }
    Ok(stats)
}

fn runtime_changes(
    baseline: &ProfileSummary,
    candidate: &ProfileSummary,
    top: usize,
) -> Vec<RuntimeChange> {
    let names = baseline
        .runtime_api_by_name
        .keys()
        .chain(candidate.runtime_api_by_name.keys())
        .cloned()
        .collect::<BTreeSet<_>>();

    let mut changes = names
        .into_iter()
        .map(|name| {
            let baseline_stat = baseline
                .runtime_api_by_name
                .get(&name)
                .cloned()
                .unwrap_or_default();
            let candidate_stat = candidate
                .runtime_api_by_name
                .get(&name)
                .cloned()
                .unwrap_or_default();
            RuntimeChange {
                name,
                delta_count: candidate_stat.count - baseline_stat.count,
                delta_time_ms: candidate_stat.time_ms - baseline_stat.time_ms,
                baseline: Stat::from(&baseline_stat),
                candidate: Stat::from(&candidate_stat),
            }
        })
        .collect::<Vec<_>>();
    changes.sort_by(|left, right| {
        right
            .delta_time_ms
            .abs()
            .partial_cmp(&left.delta_time_ms.abs())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    changes.truncate(top);
    changes
}

impl From<&NamedStat> for Stat {
    fn from(value: &NamedStat) -> Self {
        Self {
            count: value.count,
            time_ms: value.time_ms,
            bytes: value.bytes,
        }
    }
}

fn totals<'a>(profiles: impl IntoIterator<Item = &'a ProfileSummary>) -> ProfileCategories {
    let mut totals = ProfileCategories::default();
    for profile in profiles {
        totals.add(&profile.categories);
    }
    totals
}

impl ProfileCategories {
    fn add(&mut self, other: &Self) {
        self.runtime_api.add(&other.runtime_api);
        self.device_alloc_api.add(&other.device_alloc_api);
        self.host_alloc_api.add(&other.host_alloc_api);
        self.memcpy_api.add(&other.memcpy_api);
        self.sync_api.add(&other.sync_api);
        self.kernel_launch_api.add(&other.kernel_launch_api);
        self.memcpy_activity.add(&other.memcpy_activity);
        self.kernel_activity.add(&other.kernel_activity);
    }
}

impl Stat {
    fn add(&mut self, other: &Self) {
        self.count += other.count;
        self.time_ms += other.time_ms;
        self.bytes += other.bytes;
    }
}

fn render_markdown(comparison: &ProfileComparison, top: usize) -> String {
    let mut out = String::new();
    out.push_str("# Nsight Profile Comparison\n\n");
    out.push_str("## Metadata\n\n");
    out.push_str(&format!("- dataset: `{}`\n", comparison.dataset));
    out.push_str(&format!(
        "- baseline run id: `{}`\n",
        comparison.baseline_run_id
    ));
    out.push_str(&format!(
        "- candidate run id: `{}`\n",
        comparison.candidate_run_id
    ));
    out.push_str(&format!(
        "- compared queries: `{}`\n",
        comparison.compared_queries.join(", ")
    ));
    out.push_str(&format!("- baseline dir: `{}`\n", comparison.baseline_dir));
    out.push_str(&format!(
        "- candidate dir: `{}`\n\n",
        comparison.candidate_dir
    ));

    out.push_str("## Total Profile Cost\n\n");
    out.push_str("| Metric | Baseline calls | Candidate calls | Baseline ms | Candidate ms | Delta ms | Change |\n");
    out.push_str("|---|---:|---:|---:|---:|---:|---:|\n");
    category_row(
        &mut out,
        "Runtime API",
        &comparison.baseline_totals.runtime_api,
        &comparison.candidate_totals.runtime_api,
    );
    category_row(
        &mut out,
        "Device alloc/free API",
        &comparison.baseline_totals.device_alloc_api,
        &comparison.candidate_totals.device_alloc_api,
    );
    category_row(
        &mut out,
        "Host alloc/free API",
        &comparison.baseline_totals.host_alloc_api,
        &comparison.candidate_totals.host_alloc_api,
    );
    category_row(
        &mut out,
        "Memcpy API",
        &comparison.baseline_totals.memcpy_api,
        &comparison.candidate_totals.memcpy_api,
    );
    category_row(
        &mut out,
        "Sync API",
        &comparison.baseline_totals.sync_api,
        &comparison.candidate_totals.sync_api,
    );
    category_row(
        &mut out,
        "Kernel launch API",
        &comparison.baseline_totals.kernel_launch_api,
        &comparison.candidate_totals.kernel_launch_api,
    );
    category_row(
        &mut out,
        "Memcpy activity",
        &comparison.baseline_totals.memcpy_activity,
        &comparison.candidate_totals.memcpy_activity,
    );
    category_row(
        &mut out,
        "Kernel activity",
        &comparison.baseline_totals.kernel_activity,
        &comparison.candidate_totals.kernel_activity,
    );
    out.push('\n');

    out.push_str("Memcpy bytes:\n\n");
    out.push_str("| Baseline | Candidate | Delta | Change |\n");
    out.push_str("|---:|---:|---:|---:|\n");
    bytes_row(
        &mut out,
        comparison.baseline_totals.memcpy_activity.bytes,
        comparison.candidate_totals.memcpy_activity.bytes,
    );
    out.push('\n');

    out.push_str("## Per Query Deltas\n\n");
    out.push_str("| Query | Runtime API ms | Alloc/free ms | Memcpy API ms | Sync API ms | Kernel launches | Memcpy bytes |\n");
    out.push_str("|---:|---:|---:|---:|---:|---:|---:|\n");
    for query in &comparison.queries {
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} |\n",
            query.query,
            signed_ms(
                query.candidate.categories.runtime_api.time_ms
                    - query.baseline.categories.runtime_api.time_ms
            ),
            signed_ms(
                query.candidate.categories.device_alloc_api.time_ms
                    - query.baseline.categories.device_alloc_api.time_ms
            ),
            signed_ms(
                query.candidate.categories.memcpy_api.time_ms
                    - query.baseline.categories.memcpy_api.time_ms
            ),
            signed_ms(
                query.candidate.categories.sync_api.time_ms
                    - query.baseline.categories.sync_api.time_ms
            ),
            signed_count(
                query.candidate.categories.kernel_launch_api.count
                    - query.baseline.categories.kernel_launch_api.count
            ),
            signed_bytes(
                query.candidate.categories.memcpy_activity.bytes
                    - query.baseline.categories.memcpy_activity.bytes
            ),
        ));
    }
    out.push('\n');

    out.push_str("## Per Query Runtime API Changes\n\n");
    out.push_str("These are the largest CUDA runtime API deltas by absolute time.\n\n");
    for query in &comparison.queries {
        out.push_str(&format!("### {}\n\n", query.query));
        runtime_change_list(&mut out, &query.top_runtime_changes);
    }

    out.push_str("## Candidate Hotspots\n\n");
    out.push_str(
        "These rows are candidate-only and show where the candidate profile is spending time now.\n\n",
    );
    for query in &comparison.queries {
        out.push_str(&format!("### {}\n\n", query.query));
        out.push_str("Runtime API:\n\n");
        named_list(&mut out, &query.candidate.top_runtime_api, top);
        out.push_str("Memcpy activity by kind:\n\n");
        named_list(&mut out, &query.candidate.memcpy_by_kind, usize::MAX);
        out.push_str("Kernel groups:\n\n");
        kernel_group_list(&mut out, &query.candidate.kernel_groups, top);
    }

    out
}

fn category_row(out: &mut String, name: &str, baseline: &Stat, candidate: &Stat) {
    out.push_str(&format!(
        "| {name} | {} | {} | {:.1} | {:.1} | {} | {} |\n",
        baseline.count,
        candidate.count,
        baseline.time_ms,
        candidate.time_ms,
        signed_ms(candidate.time_ms - baseline.time_ms),
        percent_change(baseline.time_ms, candidate.time_ms),
    ));
}

fn bytes_row(out: &mut String, baseline: i64, candidate: i64) {
    out.push_str(&format!(
        "| {} | {} | {} | {} |\n",
        format_bytes(baseline),
        format_bytes(candidate),
        signed_bytes(candidate - baseline),
        percent_change(baseline as f64, candidate as f64),
    ));
}

fn runtime_change_list(out: &mut String, changes: &[RuntimeChange]) {
    if changes.is_empty() {
        out.push_str("No runtime API rows.\n\n");
        return;
    }

    for (index, change) in changes.iter().enumerate() {
        out.push_str(&format!(
            "{}. `{}`: {:.1} -> {:.1} ms ({}, {} -> {} calls, {})\n",
            index + 1,
            change.name,
            change.baseline.time_ms,
            change.candidate.time_ms,
            signed_ms(change.delta_time_ms),
            change.baseline.count,
            change.candidate.count,
            signed_count(change.delta_count),
        ));
    }
    out.push('\n');
}

fn named_list(out: &mut String, values: &[NamedStat], limit: usize) {
    if values.is_empty() {
        out.push_str("No rows.\n\n");
        return;
    }

    for (index, value) in values.iter().take(limit).enumerate() {
        let bytes = if value.bytes > 0 {
            format!(", {}", format_bytes(value.bytes))
        } else {
            String::new()
        };
        out.push_str(&format!(
            "{}. `{}`: {:.1} ms, {}{bytes}\n",
            index + 1,
            value.name,
            value.time_ms,
            format_calls(value.count),
        ));
    }
    out.push('\n');
}

fn kernel_group_list(out: &mut String, values: &[KernelGroup], limit: usize) {
    if values.is_empty() {
        out.push_str("No rows.\n\n");
        return;
    }

    for (index, value) in values.iter().take(limit).enumerate() {
        out.push_str(&format!(
            "{}. `{}`: {:.1} ms, {}, {}\n",
            index + 1,
            value.name,
            value.time_ms,
            format_calls(value.count),
            format_unique_full_names(value.unique_full_names),
        ));
    }
    out.push('\n');
}

fn signed_ms(value: f64) -> String {
    format!("{value:+.1}")
}

fn signed_count(value: i64) -> String {
    format!("{value:+}")
}

fn signed_bytes(value: i64) -> String {
    if value < 0 {
        format!("-{}", format_bytes(value.abs()))
    } else {
        format!("+{}", format_bytes(value))
    }
}

fn format_calls(count: i64) -> String {
    if count == 1 {
        "1 call".to_string()
    } else {
        format!("{count} calls")
    }
}

fn format_unique_full_names(count: i64) -> String {
    if count == 1 {
        "1 unique full name".to_string()
    } else {
        format!("{count} unique full names")
    }
}

fn percent_change(baseline: f64, candidate: f64) -> String {
    if baseline.abs() < f64::EPSILON {
        return "n/a".to_string();
    }
    format!("{:+.1}%", ((candidate - baseline) / baseline) * 100.0)
}

fn format_bytes(bytes: i64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;

    let bytes = bytes as f64;
    if bytes >= GIB {
        format!("{:.2} GiB", bytes / GIB)
    } else if bytes >= MIB {
        format!("{:.2} MiB", bytes / MIB)
    } else if bytes >= KIB {
        format!("{:.2} KiB", bytes / KIB)
    } else {
        format!("{bytes:.0} B")
    }
}

fn common_queries(
    baseline: &BTreeMap<String, ProfileSummary>,
    candidate: &BTreeMap<String, ProfileSummary>,
) -> Vec<String> {
    let mut queries = baseline
        .keys()
        .filter(|query| candidate.contains_key(*query))
        .cloned()
        .collect::<Vec<_>>();
    queries.sort_by_key(|query| query_sort_key(query));
    queries
}

fn query_sort_key(query: &str) -> (u32, String) {
    let number = query
        .strip_prefix('q')
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(u32::MAX);
    (number, query.to_string())
}

fn path_component(value: &str) -> String {
    value.replace(['/', '\\'], "_")
}

fn has_runtime_tables(conn: &Connection) -> Result<bool> {
    Ok(table_exists(conn, "CUPTI_ACTIVITY_KIND_RUNTIME")? && table_exists(conn, "StringIds")?)
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool> {
    sql(conn.query_row(
        "select exists(select 1 from sqlite_master where type = 'table' and name = ?1)",
        [table],
        |row| row.get::<_, bool>(0),
    ))
}

fn sql<T>(result: rusqlite::Result<T>) -> Result<T> {
    result.map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_profile_metrics_from_nsys_sqlite(
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let dir = std::env::temp_dir().join(format!(
            "libcudf-profile-compare-test-{}",
            std::process::id()
        ));
        let profiles_dir = dir.join("profiles");
        fs::create_dir_all(&profiles_dir)?;
        let db_path = profiles_dir.join("q1_gpu_nsys.sqlite");
        write_test_profile(&db_path)?;

        let profiles = load_profiles(&dir, &[], 3)?;
        let profile = profiles.get("q1").unwrap();

        assert_eq!(profile.categories.runtime_api.count, 4);
        assert_eq!(profile.categories.device_alloc_api.count, 2);
        assert_eq!(profile.categories.memcpy_api.count, 1);
        assert_eq!(profile.categories.memcpy_activity.bytes, 4096);
        assert_eq!(profile.categories.kernel_activity.count, 1);
        assert_eq!(
            profile.top_runtime_api[0].name,
            "cudaStreamSynchronize_v3020"
        );
        assert_eq!(profile.memcpy_by_kind[0].name, "Host-to-Device");
        assert_eq!(profile.kernel_groups[0].name, "kernel_short");
        assert_eq!(profile.kernel_groups[0].unique_full_names, 1);

        fs::remove_dir_all(dir)?;
        Ok(())
    }

    #[test]
    fn sorts_runtime_changes_by_absolute_delta() {
        let mut baseline = ProfileSummary::default();
        baseline.runtime_api_by_name.insert(
            "cudaMalloc_v3020".to_string(),
            NamedStat {
                name: "cudaMalloc_v3020".to_string(),
                count: 10,
                time_ms: 50.0,
                bytes: 0,
            },
        );
        baseline.runtime_api_by_name.insert(
            "cudaMemcpyAsync_v3020".to_string(),
            NamedStat {
                name: "cudaMemcpyAsync_v3020".to_string(),
                count: 5,
                time_ms: 10.0,
                bytes: 0,
            },
        );

        let mut candidate = ProfileSummary::default();
        candidate.runtime_api_by_name.insert(
            "cudaMalloc_v3020".to_string(),
            NamedStat {
                name: "cudaMalloc_v3020".to_string(),
                count: 1,
                time_ms: 1.0,
                bytes: 0,
            },
        );
        candidate.runtime_api_by_name.insert(
            "cudaMemcpyAsync_v3020".to_string(),
            NamedStat {
                name: "cudaMemcpyAsync_v3020".to_string(),
                count: 5,
                time_ms: 12.0,
                bytes: 0,
            },
        );

        let changes = runtime_changes(&baseline, &candidate, 1);

        assert_eq!(changes[0].name, "cudaMalloc_v3020");
        assert_eq!(changes[0].delta_count, -9);
        assert_eq!(changes[0].delta_time_ms, -49.0);
    }

    #[test]
    fn formats_signed_values() {
        assert_eq!(signed_ms(-5.25), "-5.2");
        assert_eq!(signed_count(10), "+10");
        assert_eq!(signed_bytes(0), "+0 B");
        assert_eq!(format_calls(1), "1 call");
        assert_eq!(format_unique_full_names(2), "2 unique full names");
    }

    #[test]
    fn rejects_zero_top() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let opt = ProfileCompareOpt {
            dataset: "tpch_sf1".to_string(),
            baseline_run_id: "before".to_string(),
            candidate_run_id: "after".to_string(),
            query: Vec::new(),
            output: PathBuf::from("benchmark-results"),
            top: 0,
        };

        let err = opt.run().expect_err("zero top should be rejected");

        assert!(err.to_string().contains("--top must be greater than 0"));
        Ok(())
    }

    fn write_test_profile(path: &Path) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "
            create table StringIds(id integer primary key, value text not null);
            insert into StringIds values
              (1, 'cudaMalloc_v3020'),
              (2, 'cudaFree_v3020'),
              (3, 'cudaMemcpyAsync_v3020'),
              (4, 'cudaStreamSynchronize_v3020'),
              (5, 'kernel_demangled'),
              (6, 'kernel_short');

            create table CUPTI_ACTIVITY_KIND_RUNTIME(
              start integer not null,
              end integer not null,
              nameId integer not null
            );
            insert into CUPTI_ACTIVITY_KIND_RUNTIME values
              (0, 1000000, 1),
              (0, 2000000, 2),
              (0, 3000000, 3),
              (0, 4000000, 4);

            create table ENUM_CUDA_MEMCPY_OPER(
              id integer primary key,
              label text
            );
            insert into ENUM_CUDA_MEMCPY_OPER values (1, 'Host-to-Device');

            create table CUPTI_ACTIVITY_KIND_MEMCPY(
              start integer not null,
              end integer not null,
              bytes integer not null,
              copyKind integer not null
            );
            insert into CUPTI_ACTIVITY_KIND_MEMCPY values (0, 5000000, 4096, 1);

            create table CUPTI_ACTIVITY_KIND_KERNEL(
              start integer not null,
              end integer not null,
              demangledName integer not null,
              shortName integer not null,
              mangledName integer
            );
            insert into CUPTI_ACTIVITY_KIND_KERNEL values (0, 6000000, 5, 6, null);
            ",
        )?;
        Ok(())
    }
}
