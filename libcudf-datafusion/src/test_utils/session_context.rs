use crate::SessionStateBuilderExt;
use arrow::array::{ArrayRef, Float64Array, RecordBatch};
use arrow::util::pretty::pretty_format_batches;
use datafusion::error::DataFusionError;
use datafusion::execution::{SessionStateBuilder, TaskContext};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_physical_plan::{displayable, execute_stream, ExecutionPlan};
use futures_util::TryStreamExt;
use std::path::PathBuf;
use std::sync::Arc;

pub struct TestFramework {
    ctx: SessionContext,
}

impl TestFramework {
    pub async fn new() -> Self {
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(SessionConfig::new().with_target_partitions(4))
            .with_cudf_planner()
            .build();
        let ctx = SessionContext::from(state);

        crate::test_utils::tpch::register_cudf_aggregate_udfs(&ctx);

        let mut base = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        base.pop();
        ctx.register_parquet(
            "weather",
            format!("{}/testdata/weather/", base.display()),
            ParquetReadOptions::new(),
        )
        .await
        .expect("Cannot register parquet datasource");
        Self { ctx }
    }

    pub async fn plan(&self, sql: &str) -> Result<TestPlan, DataFusionError> {
        let mut prepare_statements = sql.split(";").collect::<Vec<_>>();
        let sql = prepare_statements.pop().unwrap();
        for sql in prepare_statements {
            self.ctx.sql(sql).await?;
        }
        let df = self.ctx.sql(sql).await?;
        let plan = df.create_physical_plan().await?;
        Ok(TestPlan {
            plan,
            ctx: self.ctx.task_ctx(),
        })
    }

    pub async fn execute(&self, sql: &str) -> Result<SqlResult, DataFusionError> {
        self.plan(sql).await?.execute().await
    }

    pub fn task_ctx(&self) -> Arc<TaskContext> {
        self.ctx.task_ctx()
    }
}

pub struct TestPlan {
    pub plan: Arc<dyn ExecutionPlan>,
    ctx: Arc<TaskContext>,
}

impl TestPlan {
    pub async fn execute(&self) -> Result<SqlResult, DataFusionError> {
        let stream = execute_stream(self.plan.clone(), self.ctx.clone())?;
        let batches = stream.try_collect::<Vec<_>>().await?;
        Ok(SqlResult {
            pretty_print: pretty_format_batches(&batches)?.to_string(),
            plan: self.display(),
            batches,
        })
    }

    pub fn display(&self) -> String {
        displayable(self.plan.as_ref()).indent(true).to_string()
    }
}

pub struct SqlResult {
    pub pretty_print: String,
    pub plan: String,
    pub batches: Vec<RecordBatch>,
}

/// Run `sql` on both GPU and CPU with `partitions` target partitions, assert results match,
/// and return the GPU [`SqlResult`].
pub(crate) async fn check_query_results(
    sql: &str,
    partitions: usize,
) -> Result<SqlResult, DataFusionError> {
    let tf = TestFramework::new().await;
    let cudf_sql = format!(
        "SET cudf.enable=true; SET datafusion.execution.target_partitions={partitions}; {sql}"
    );
    let cpu_sql = format!("SET datafusion.execution.target_partitions={partitions}; {sql}");
    let gpu = tf.execute(&cudf_sql).await?;
    let cpu = tf.execute(&cpu_sql).await?;
    let gpu_pp = pretty_format_rounded(&sort_batches(&gpu.batches), 10)?;
    let cpu_pp = pretty_format_rounded(&sort_batches(&cpu.batches), 10)?;
    assert_eq!(gpu_pp, cpu_pp);
    Ok(gpu)
}

/// Pretty-print `batches` after rounding every Float64 column to `decimals`
/// decimal places. Lets the ASCII table renderer pick consistent column
/// widths on both the GPU and CPU sides, absorbing last-ULP float
/// differences between cuDF and DataFusion.
fn pretty_format_rounded(
    batches: &[RecordBatch],
    decimals: u32,
) -> Result<String, DataFusionError> {
    let factor = 10f64.powi(decimals as i32);
    let rounded: Vec<RecordBatch> = batches
        .iter()
        .map(|batch| {
            let columns: Vec<ArrayRef> = batch
                .columns()
                .iter()
                .map(|col| {
                    if let Some(floats) = col.as_any().downcast_ref::<Float64Array>() {
                        let rounded: Float64Array = floats
                            .iter()
                            .map(|v| v.map(|x| (x * factor).round() / factor))
                            .collect();
                        Arc::new(rounded) as ArrayRef
                    } else {
                        Arc::clone(col)
                    }
                })
                .collect();
            RecordBatch::try_new(batch.schema(), columns)
                .expect("RecordBatch::try_new failed")
        })
        .collect();
    Ok(pretty_format_batches(&rounded)?.to_string())
}

/// Concatenate `batches` into one and sort rows by the first column.
pub(crate) fn sort_batches(batches: &[RecordBatch]) -> Vec<RecordBatch> {
    if batches.is_empty() {
        return vec![];
    }
    let schema = batches[0].schema();
    let combined = arrow::compute::concat_batches(&schema, batches).expect("concat_batches failed");
    if combined.num_rows() == 0 {
        return vec![combined];
    }
    let indices =
        arrow::compute::sort_to_indices(combined.column(0), None, None).expect("sort failed");
    let columns: Vec<_> = combined
        .columns()
        .iter()
        .map(|c| arrow::compute::take(c.as_ref(), &indices, None).expect("take failed"))
        .collect();
    vec![RecordBatch::try_new(schema, columns).expect("RecordBatch::try_new failed")]
}
