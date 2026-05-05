#[cfg(all(feature = "clickbench", test))]
mod tests {
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::execute_stream;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use futures::TryStreamExt;
    use libcudf_datafusion::aggregate::{avg, count, max, min, sum};
    use libcudf_datafusion::SessionStateBuilderExt;
    use libcudf_datafusion_benchmarks::datasets::{clickbench, register_tables};
    use std::error::Error;
    use std::fmt::Display;
    use std::ops::Range;
    use std::path::Path;
    use tokio::sync::OnceCell;

    const PARTITIONS: usize = 6;
    const FILE_RANGE: Range<usize> = 0..3;

    #[tokio::test]
    #[ignore = "Arrow error: Column 'count(Int64(1))' is declared as non-nullable but contains null values"]
    async fn test_clickbench_0() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q0").await
    }

    #[tokio::test]
    #[ignore = "GPU and CPU result sets do not match"]
    async fn test_clickbench_1() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q1").await
    }

    #[tokio::test]
    async fn test_clickbench_2() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q2").await
    }

    #[tokio::test]
    #[ignore = "Flaky under parallel test execution"]
    async fn test_clickbench_3() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q3").await
    }

    #[tokio::test]
    async fn test_clickbench_4() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q4").await
    }

    #[tokio::test]
    #[ignore = "CUDF failure: Unsupported type_id conversion to cudf"]
    async fn test_clickbench_5() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q5").await
    }

    #[tokio::test]
    #[ignore = "Arrow error: Expected all Arrays in RecordBatch to be CuDFColumnView"]
    async fn test_clickbench_6() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q6").await
    }

    #[tokio::test]
    async fn test_clickbench_7() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q7").await
    }

    #[tokio::test]
    async fn test_clickbench_8() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q8").await
    }

    #[tokio::test]
    async fn test_clickbench_9() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q9").await
    }

    #[tokio::test]
    #[ignore = "CUDF failure: Unsupported type_id conversion to cudf"]
    async fn test_clickbench_10() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q10").await
    }

    #[tokio::test]
    #[ignore = "CUDF failure: Unsupported type_id conversion to cudf"]
    async fn test_clickbench_11() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q11").await
    }

    #[tokio::test]
    #[ignore = "CUDF failure: Unsupported type_id conversion to cudf"]
    async fn test_clickbench_12() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q12").await
    }

    #[tokio::test]
    #[ignore = "CUDF failure: Unsupported type_id conversion to cudf"]
    async fn test_clickbench_13() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q13").await
    }

    #[tokio::test]
    #[ignore = "CUDF failure: Unsupported type_id conversion to cudf"]
    async fn test_clickbench_14() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q14").await
    }

    #[tokio::test]
    async fn test_clickbench_15() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q15").await
    }

    #[tokio::test]
    #[ignore = "CUDF failure: Unsupported type_id conversion to cudf"]
    async fn test_clickbench_16() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q16").await
    }

    #[tokio::test]
    #[ignore = "CUDF failure: Unsupported type_id conversion to cudf"]
    async fn test_clickbench_17() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q17").await
    }

    #[tokio::test]
    #[ignore = "CUDF failure: Unsupported type_id conversion to cudf"]
    async fn test_clickbench_18() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q18").await
    }

    #[tokio::test]
    async fn test_clickbench_19() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q19").await
    }

    #[tokio::test]
    async fn test_clickbench_20() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q20").await
    }

    #[tokio::test]
    #[ignore = "CUDF failure: Unsupported type_id conversion to cudf"]
    async fn test_clickbench_21() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q21").await
    }

    #[tokio::test]
    #[ignore = "CUDF failure: Unsupported type_id conversion to cudf"]
    async fn test_clickbench_22() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q22").await
    }

    #[tokio::test]
    #[ignore = "CUDF failure: Unsupported type_id conversion to cudf"]
    async fn test_clickbench_23() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q23").await
    }

    #[tokio::test]
    #[ignore = "CUDF failure: Unsupported type_id conversion to cudf"]
    async fn test_clickbench_24() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q24").await
    }

    #[tokio::test]
    #[ignore = "CUDF failure: Unsupported type_id conversion to cudf"]
    async fn test_clickbench_25() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q25").await
    }

    #[tokio::test]
    #[ignore = "CUDF failure: Unsupported type_id conversion to cudf"]
    async fn test_clickbench_26() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q26").await
    }

    #[tokio::test]
    #[ignore = "CUDF failure: Unsupported type_id conversion to cudf"]
    async fn test_clickbench_27() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q27").await
    }

    #[tokio::test]
    #[ignore = "CUDF failure: Unsupported type_id conversion to cudf"]
    async fn test_clickbench_28() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q28").await
    }

    #[tokio::test]
    async fn test_clickbench_29() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q29").await
    }

    #[tokio::test]
    #[ignore = "CUDF failure: Unsupported type_id conversion to cudf"]
    async fn test_clickbench_30() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q30").await
    }

    #[tokio::test]
    #[ignore = "CUDF failure: Unsupported type_id conversion to cudf"]
    async fn test_clickbench_31() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q31").await
    }

    #[tokio::test]
    #[ignore = "GPU and CPU result sets do not match"]
    async fn test_clickbench_32() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q32").await
    }

    #[tokio::test]
    #[ignore = "CUDF failure: Unsupported type_id conversion to cudf"]
    async fn test_clickbench_33() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q33").await
    }

    #[tokio::test]
    #[ignore = "CUDF failure: Unsupported type_id conversion to cudf"]
    async fn test_clickbench_34() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q34").await
    }

    #[tokio::test]
    #[ignore = "Panic: as_primitive cast in cuDF aggregate output"]
    async fn test_clickbench_35() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q35").await
    }

    #[tokio::test]
    async fn test_clickbench_36() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q36").await
    }

    #[tokio::test]
    async fn test_clickbench_37() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q37").await
    }

    #[tokio::test]
    async fn test_clickbench_38() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q38").await
    }

    #[tokio::test]
    async fn test_clickbench_39() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q39").await
    }

    #[tokio::test]
    async fn test_clickbench_40() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q40").await
    }

    #[tokio::test]
    async fn test_clickbench_41() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q41").await
    }

    #[tokio::test]
    async fn test_clickbench_42() -> Result<(), Box<dyn Error>> {
        test_clickbench_query("q42").await
    }

    /// Run `query_id` on the GPU pipeline and against a stock CPU `SessionContext`,
    /// then assert that the rendered tables match.
    async fn test_clickbench_query(query_id: &str) -> Result<(), Box<dyn Error>> {
        let sql = clickbench::get_query(query_id)?;

        let gpu_ctx = SessionContext::from(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(SessionConfig::new().with_target_partitions(PARTITIONS))
                .with_cudf_planner()
                .build(),
        );
        register_cudf_aggregate_udfs(&gpu_ctx);

        let results_gpu = run_clickbench_query(gpu_ctx, sql.clone()).await?;
        let results_host = run_clickbench_query(SessionContext::new(), sql).await?;

        pretty_assertions::assert_eq!(results_gpu.to_string(), results_host.to_string());
        Ok(())
    }

    async fn run_clickbench_query(
        ctx: SessionContext,
        sql: String,
    ) -> Result<impl Display, Box<dyn Error>> {
        let data_dir = ensure_clickbench_data(FILE_RANGE).await;

        register_tables(&ctx, &data_dir).await?;

        let df = ctx.sql(&sql).await?;
        let plan = df.create_physical_plan().await?;
        let stream = execute_stream(plan, ctx.task_ctx())?;
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

    static INIT_TEST_CLICKBENCH_TABLES: OnceCell<()> = OnceCell::const_new();

    async fn ensure_clickbench_data(range: Range<usize>) -> std::path::PathBuf {
        let data_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
            "data/clickbench/correctness_range{}-{}",
            range.start, range.end
        ));
        INIT_TEST_CLICKBENCH_TABLES
            .get_or_init(|| async {
                clickbench::generate_clickbench_data(&data_dir, range)
                    .await
                    .unwrap();
            })
            .await;
        data_dir
    }
}
