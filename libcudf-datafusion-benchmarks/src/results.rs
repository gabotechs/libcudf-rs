use crate::{DATA_PATH, RESULTS_DIR};
use datafusion::common::utils::get_available_parallelism;
use datafusion::common::{internal_datafusion_err, Result};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime};

/// A single iteration of a benchmark query
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryIter {
    pub row_count: usize,
    #[serde(
        serialize_with = "serialize_elapsed",
        deserialize_with = "deserialize_elapsed"
    )]
    pub elapsed: Duration,
    pub error: Option<String>,
}

/// A single benchmark case
#[derive(Debug, Serialize, Deserialize)]
pub struct BenchResult {
    pub id: String,
    pub dataset: String,
    pub iterations: Vec<QueryIter>,
}

/// collects benchmark run data and then serializes it at the end
#[derive(Debug, Serialize, Deserialize)]
pub struct BenchmarkRun {
    /// Whether the run used the GPU execution path
    pub gpu: bool,
    /// Start time
    #[serde(
        serialize_with = "serialize_start_time",
        deserialize_with = "deserialize_start_time"
    )]
    pub start_time: SystemTime,
    pub dataset: String,
    pub branch: String,
    #[serde(serialize_with = "serialize_bench_results")]
    pub results: Vec<BenchResult>,
}

impl BenchmarkRun {
    pub fn new(dataset: String, gpu: bool) -> Self {
        Self {
            gpu,
            dataset,
            branch: get_current_branch(),
            start_time: SystemTime::now(),
            results: vec![],
        }
    }

    pub fn load_previous(dataset: &str) -> Option<Self> {
        let path = PathBuf::from(DATA_PATH).join(dataset).join("previous.json");
        let Ok(prev) = fs::read(path) else {
            return None;
        };

        let Ok(mut prev_output) = serde_json::from_slice::<Self>(&prev) else {
            return None;
        };

        prev_output.results = BenchResult::load_many(&prev_output.dataset, &prev_output.branch);
        Some(prev_output)
    }

    /// Write data as json into output path if it exists.
    pub fn store(&self) -> Result<()> {
        let path = PathBuf::from(DATA_PATH)
            .join(&self.dataset)
            .join("previous.json");
        let json = serde_json::to_string_pretty(&self).unwrap();

        let _ = fs::create_dir_all(path.parent().unwrap());

        fs::write(path, json)?;
        for result in &self.results {
            result.store()?;
        }
        Ok(())
    }

    pub fn store_results_to_dir(&self, dir: &Path) -> Result<()> {
        for result in &self.results {
            result.store_to_dir(dir)?;
        }
        Ok(())
    }

    pub fn compare_with_previous(&self) -> Result<()> {
        let Some(previous) = Self::load_previous(&self.dataset) else {
            return Ok(());
        };

        let header = format!(
            "=== Comparing {} results from branch '{}' [prev] with '{}' [new] ===",
            self.dataset, previous.branch, self.branch
        );
        println!("{header}");
        println!("os:        {}", std::env::consts::OS);
        println!("arch:      {}", std::env::consts::ARCH);
        println!("cpu cores: {}", get_available_parallelism());
        println!("gpu:       {} -> {}", previous.gpu, self.gpu);
        println!("{}", "=".repeat(header.len()));
        for query in self.results.iter() {
            let Some(prev_query) = previous.results.iter().find(|v| v.id == query.id) else {
                continue;
            };
            query.compare(prev_query)
        }

        Ok(())
    }
}

fn get_current_branch() -> String {
    let output = Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .output()
        .expect("failed to execute git command");

    let branch_name = String::from_utf8(output.stdout)
        .expect("git output is not valid UTF-8")
        .trim()
        .to_string();

    branch_name.split("/").last().unwrap().to_string()
}

impl BenchResult {
    pub fn avg(&self) -> u128 {
        self.iterations
            .iter()
            .map(|v| v.elapsed.as_millis())
            .sum::<u128>()
            / self.iterations.len() as u128
    }

    pub fn store(&self) -> Result<()> {
        let path = PathBuf::from(DATA_PATH)
            .join(&self.dataset)
            .join(RESULTS_DIR)
            .join(get_current_branch())
            .join(format!("{}.json", self.id));

        let _ = fs::create_dir_all(path.parent().unwrap());

        let result_string =
            serde_json::to_string_pretty(self).map_err(|err| internal_datafusion_err!("{err}"))?;
        fs::write(path, result_string)?;

        Ok(())
    }

    pub fn store_to_dir(&self, dir: &Path) -> Result<()> {
        fs::create_dir_all(dir)?;
        let result_string =
            serde_json::to_string_pretty(self).map_err(|err| internal_datafusion_err!("{err}"))?;
        fs::write(
            dir.join(format!("{}.json", result_file_stem(&self.id))),
            result_string,
        )?;
        Ok(())
    }

    pub fn load_many(dataset: &str, branch: &str) -> Vec<Self> {
        let dir = PathBuf::from(DATA_PATH)
            .join(dataset)
            .join(RESULTS_DIR)
            .join(branch);

        let Ok(dir) = fs::read_dir(dir) else {
            return vec![];
        };

        let mut results = vec![];
        for file in dir {
            let Ok(file) = file else { continue };
            let file_name = file.file_name().to_string_lossy().to_string();
            let id = if file_name.ends_with(".json") {
                file_name.trim_end_matches(".json")
            } else {
                continue;
            };
            let Ok(result) = BenchResult::load(dataset, branch, id) else {
                continue;
            };
            results.push(result);
        }
        results.sort_by(|a, b| {
            let extract_number = |s: &str| -> Option<u32> {
                s.chars()
                    .filter(|c| c.is_ascii_digit())
                    .collect::<String>()
                    .parse::<u32>()
                    .ok()
            };

            match (extract_number(&a.id), extract_number(&b.id)) {
                (Some(num_a), Some(num_b)) => num_a.cmp(&num_b),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => a.id.cmp(&b.id),
            }
        });
        results
    }

    pub fn load(dataset: &str, branch: &str, id: &str) -> Result<Self> {
        let path = PathBuf::from(DATA_PATH)
            .join(dataset)
            .join(RESULTS_DIR)
            .join(branch)
            .join(format!("{id}.json"));

        let read = fs::read(path)?;
        let read =
            serde_json::from_slice(&read).map_err(|err| internal_datafusion_err!("{err}"))?;
        Ok(read)
    }

    pub fn compare(&self, prev_query: &Self) {
        let prev_err = prev_query.iterations.iter().find_map(|v| v.error.clone());
        let new_err = self.iterations.iter().find_map(|v| v.error.clone());
        match (prev_err, new_err) {
            (Some(_prev_err), None) => {
                println!("{}: Previously failed, but now succeeded 🟠", self.id);
                return;
            }
            (None, Some(_new_err)) => {
                println!("{}: Previously succeeded, but now failed ❌", self.id);
                return;
            }
            (Some(_prev_err), Some(_new_err)) => {
                println!("{}: Previously failed, and now also failed ❌", self.id);
                return;
            }
            (None, None) => {}
        }

        let avg_prev = prev_query.avg();
        let avg = self.avg();
        let (f, tag, emoji) = if avg < avg_prev {
            let f = avg_prev as f64 / avg as f64;
            (f, "faster", if f > 1.2 { "✅" } else { "✔" })
        } else {
            let f = avg as f64 / avg_prev as f64;
            (f, "slower", if f > 1.2 { "❌" } else { "✖" })
        };
        println!(
            "{:>8}: prev={avg_prev:>4} ms, new={avg:>4} ms, diff={f:.2} {tag} {emoji}",
            self.id
        );
    }
}

fn result_file_stem(id: &str) -> &str {
    id.rsplit_once(' ').map_or(id, |(_, query)| query)
}

fn serialize_bench_results<S: Serializer>(
    _bench_result: &[BenchResult],
    ser: S,
) -> Result<S::Ok, S::Error> {
    ser.serialize_seq(Some(0))?.end()
}

fn serialize_start_time<S>(start_time: &SystemTime, ser: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    ser.serialize_u64(
        start_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("current time is later than UNIX_EPOCH")
            .as_secs(),
    )
}
fn deserialize_start_time<'de, D>(des: D) -> Result<SystemTime, D::Error>
where
    D: Deserializer<'de>,
{
    let secs = u64::deserialize(des)?;
    Ok(SystemTime::UNIX_EPOCH + Duration::from_secs(secs))
}

fn serialize_elapsed<S>(elapsed: &Duration, ser: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let ms = elapsed.as_secs_f64() * 1000.0;
    ser.serialize_f64(ms)
}

fn deserialize_elapsed<'de, D>(des: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let ms = f64::deserialize(des)?;
    Ok(Duration::from_secs_f64(ms / 1000.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_dir_uses_query_file_name() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let dir =
            std::env::temp_dir().join(format!("libcudf-rs-result-dir-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);

        let result = BenchResult {
            id: "tpch_sf1 q22".to_string(),
            dataset: "tpch_sf1".to_string(),
            iterations: vec![],
        };
        result.store_to_dir(&dir)?;

        assert!(dir.join("q22.json").exists());
        assert!(!dir.join("tpch_sf1 q22.json").exists());

        fs::remove_dir_all(dir)?;
        Ok(())
    }
}
