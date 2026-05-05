#[cfg(all(feature = "tpch", test))]
mod tests {
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::execute_stream;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use futures::TryStreamExt;
    use libcudf_datafusion::aggregate::{avg, count, max, min, sum};
    use libcudf_datafusion::SessionStateBuilderExt;
    use libcudf_datafusion_benchmarks::datasets::{register_tables, tpch};
    use std::error::Error;
    use std::fmt::Display;
    use std::fs;
    use std::path::Path;
    use tokio::sync::OnceCell;

    const PARTITIONS: usize = 6;
    const TPCH_SCALE_FACTOR: f64 = 1.0;
    const TPCH_DATA_PARTS: i32 = 16;

    #[tokio::test]
    #[ignore = "cuDF aggregate path currently panics converting decimal aggregate state back through Arrow"]
    async fn test_tpch_1() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q1").await
    }

    #[tokio::test]
    async fn test_tpch_2() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q2").await
    }

    #[tokio::test]
    #[ignore = "cuDF aggregate path currently panics converting decimal aggregate state back through Arrow"]
    async fn test_tpch_3() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q3").await
    }

    #[tokio::test]
    async fn test_tpch_4() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q4").await
    }

    #[tokio::test]
    #[ignore = "cuDF aggregate path currently panics converting decimal aggregate state back through Arrow"]
    async fn test_tpch_5() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q5").await
    }

    #[tokio::test]
    async fn test_tpch_6() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q6").await
    }

    #[tokio::test]
    async fn test_tpch_7() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q7").await
    }

    #[tokio::test]
    async fn test_tpch_8() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q8").await
    }

    #[tokio::test]
    async fn test_tpch_9() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q9").await
    }

    #[tokio::test]
    #[ignore = "cuDF aggregate path currently panics converting decimal aggregate state back through Arrow"]
    async fn test_tpch_10() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q10").await
    }

    #[tokio::test]
    async fn test_tpch_11() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q11").await
    }

    #[tokio::test]
    async fn test_tpch_12() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q12").await
    }

    #[tokio::test]
    async fn test_tpch_13() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q13").await
    }

    #[tokio::test]
    async fn test_tpch_14() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q14").await
    }

    #[tokio::test]
    #[ignore = "cuDF aggregate path currently panics converting decimal aggregate state back through Arrow"]
    async fn test_tpch_15() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q15").await
    }

    #[tokio::test]
    async fn test_tpch_16() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q16").await
    }

    #[tokio::test]
    #[ignore = "cuDF aggregate path currently reaches an unsupported cuDF decimal binary op"]
    async fn test_tpch_17() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q17").await
    }

    #[tokio::test]
    async fn test_tpch_18() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q18").await
    }

    #[tokio::test]
    async fn test_tpch_19() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q19").await
    }

    #[tokio::test]
    async fn test_tpch_20() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q20").await
    }

    #[tokio::test]
    async fn test_tpch_21() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q21").await
    }

    #[tokio::test]
    async fn test_tpch_22() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q22").await
    }

    /// Run `query_id` on the GPU pipeline and against a stock CPU `SessionContext`,
    /// then assert that the rendered tables match.
    async fn test_tpch_query(query_id: &str) -> Result<(), Box<dyn Error>> {
        let mut sql = tpch::get_query(query_id)?;
        if query_id == "q10" {
            // Q10 can return non-deterministic results when two entries have the
            // same revenue; this extra ordering pins the row order.
            sql = sql.replace("revenue desc", "revenue, c_acctbal desc");
        }
        let gpu_ctx = SessionContext::from(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(SessionConfig::new().with_target_partitions(PARTITIONS))
                .with_cudf_planner()
                .build(),
        );
        register_cudf_aggregate_udfs(&gpu_ctx);

        let results_gpu = run_tpch_query(gpu_ctx, sql.clone()).await?;
        let results_host = run_tpch_query(SessionContext::new(), sql).await?;

        pretty_assertions::assert_eq!(results_gpu.to_string(), results_host.to_string());
        Ok(())
    }

    async fn run_tpch_query(
        ctx: SessionContext,
        sql: String,
    ) -> Result<impl Display, Box<dyn Error>> {
        let data_dir = ensure_tpch_data(TPCH_SCALE_FACTOR, TPCH_DATA_PARTS).await;

        register_tables(&ctx, &data_dir).await?;

        // Query 15 has three queries in it, one creating the view, the second
        // executing, which we want to capture the output of, and the third
        // tearing down the view
        let stream = if sql.starts_with("create view") {
            let queries: Vec<&str> = sql
                .split(';')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect();

            ctx.sql(queries[0]).await?.collect().await?;
            let df = ctx.sql(queries[1]).await?;
            let plan = df.create_physical_plan().await?;
            let stream = execute_stream(plan.clone(), ctx.task_ctx())?;
            ctx.sql(queries[2]).await?.collect().await?;

            stream
        } else {
            let df = ctx.sql(&sql).await?;
            let plan = df.create_physical_plan().await?;
            execute_stream(plan.clone(), ctx.task_ctx())?
        };

        let batches = stream.try_collect::<Vec<_>>().await?;
        Ok(arrow::util::pretty::pretty_format_batches(&batches)?)
    }

    fn register_cudf_aggregate_udfs(ctx: &SessionContext) {
        ctx.register_udaf((*avg()).clone());
        ctx.register_udaf((*count()).clone());
        ctx.register_udaf((*max()).clone());
        ctx.register_udaf((*min()).clone());
        ctx.register_udaf((*sum()).clone());
    }

    static INIT_TEST_TPCH_TABLES: OnceCell<()> = OnceCell::const_new();

    async fn ensure_tpch_data(sf: f64, parts: i32) -> std::path::PathBuf {
        let data_dir =
            Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("data/tpch/correctness_sf{sf}"));
        INIT_TEST_TPCH_TABLES
            .get_or_init(|| async {
                if !fs::exists(&data_dir).unwrap() {
                    tpch::generate_tpch_data(&data_dir, sf, parts);
                }
            })
            .await;
        data_dir
    }
}
