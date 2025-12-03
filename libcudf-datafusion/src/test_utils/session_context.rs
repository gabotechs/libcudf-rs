use crate::optimizer::{CuDFBoundariesRule, CuDFConfig, HostToCuDFRule};
use arrow::util::pretty::pretty_format_batches;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_physical_plan::{displayable, execute_stream};
use futures_util::TryStreamExt;
use std::path::PathBuf;
use std::sync::Arc;

pub struct TestFramework {
    ctx: SessionContext,
}

impl TestFramework {
    pub async fn new() -> Self {
        let config = SessionConfig::new().with_option_extension(CuDFConfig::default());

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_physical_optimizer_rule(Arc::new(HostToCuDFRule))
            .with_physical_optimizer_rule(Arc::new(CuDFBoundariesRule))
            .build();
        let ctx = SessionContext::from(state);
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

    pub async fn sql(&self, sql: &str) -> Result<SqlResult, DataFusionError> {
        let mut prepare_statements = sql.split(";").collect::<Vec<_>>();
        let sql = prepare_statements.pop().unwrap();
        for sql in prepare_statements {
            self.ctx.sql(sql).await?;
        }
        let df = self.ctx.sql(sql).await?;
        let plan = df.create_physical_plan().await?;
        let plan_str = displayable(plan.as_ref()).indent(true).to_string();
        let stream = execute_stream(plan, self.ctx.task_ctx())?;
        let batches = stream.try_collect::<Vec<_>>().await?;
        Ok(SqlResult {
            pretty_print: pretty_format_batches(&batches)?.to_string(),
            plan: plan_str,
        })
    }
}

pub struct SqlResult {
    pub pretty_print: String,
    pub plan: String,
}
