use datafusion::common::{internal_datafusion_err, internal_err, DataFusionError};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use std::fs;
use std::path::Path;

/// Returns the directory of this crate. Query files are stored under `testdata/`
/// inside the benchmarks crate itself.
fn crate_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

pub fn get_queries(path: &str) -> Vec<String> {
    let queries_dir = crate_root().join(path);
    let mut result = vec![];
    let Ok(read_dir) = queries_dir.read_dir() else {
        return result;
    };
    for file in read_dir {
        let file = file.unwrap();
        let file_name = file.file_name().display().to_string();
        if file_name.ends_with(".sql") {
            result.push(file_name.trim_end_matches(".sql").to_string());
        }
    }

    // Each element might be something like q12.sql or custom2.sql.
    // This orders the string list by the parsed integer number inside an arbitrary string.
    result.sort_by(|a, b| {
        let extract_number = |s: &str| -> Option<u32> {
            s.chars()
                .filter(|c| c.is_ascii_digit())
                .collect::<String>()
                .parse::<u32>()
                .ok()
        };

        match (extract_number(a), extract_number(b)) {
            (Some(num_a), Some(num_b)) => num_a.cmp(&num_b),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => a.cmp(b),
        }
    });
    result
}

pub fn get_query(path: &str, id: &str) -> Result<String, DataFusionError> {
    let queries_dir = crate_root().join(path);

    if !queries_dir.exists() {
        return internal_err!(
            "Benchmark queries directory not found: {}",
            queries_dir.display()
        );
    }

    let query_file = queries_dir.join(format!("{id}.sql"));

    if !query_file.exists() {
        return internal_err!("Query file not found: {}", query_file.display());
    }

    let query_sql = fs::read_to_string(&query_file)
        .map_err(|e| {
            internal_datafusion_err!("Failed to read query file {}: {e}", query_file.display())
        })?
        .trim()
        .to_string();

    Ok(query_sql)
}

/// Applies DataFusion settings declared as SQL comments in benchmark queries.
///
/// For example, a query can include
/// `-- set datafusion.execution.parquet.binary_as_string = true`.
pub async fn apply_query_settings(
    ctx: &SessionContext,
    query_sql: &str,
) -> Result<(), DataFusionError> {
    for statement in query_setting_statements(query_sql) {
        ctx.sql(&statement).await?;
    }
    Ok(())
}

pub async fn register_tables(
    ctx: &SessionContext,
    data_path: &Path,
) -> Result<(), DataFusionError> {
    for entry in fs::read_dir(data_path)? {
        let path = entry?.path();
        if path.is_dir() {
            let table_name = path.file_name().unwrap().to_str().unwrap();
            ctx.register_parquet(
                table_name,
                path.to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await?;
        }
    }
    Ok(())
}

fn query_setting_statements(query_sql: &str) -> impl Iterator<Item = String> + '_ {
    query_sql.lines().filter_map(|line| {
        let directive = line.trim().strip_prefix("--")?.trim_start();
        let setting = directive.strip_prefix("set ")?;
        let setting = setting.trim().trim_end_matches(';').trim();
        (!setting.is_empty()).then(|| format!("SET {setting}"))
    })
}

#[cfg(test)]
mod tests {
    use super::query_setting_statements;

    #[test]
    fn test_query_setting_statements() -> Result<(), Box<dyn std::error::Error>> {
        let statements = query_setting_statements(
            "-- ignored\n\
             -- set datafusion.execution.parquet.binary_as_string = true;\n\
             SELECT * FROM hits",
        )
        .collect::<Vec<_>>();

        assert_eq!(
            statements,
            vec!["SET datafusion.execution.parquet.binary_as_string = true"]
        );
        Ok(())
    }
}
