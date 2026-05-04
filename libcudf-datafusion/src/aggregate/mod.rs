use arrow_schema::Schema;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::projection::ProjectionMapping;
use datafusion_physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion_physical_plan::udaf::AggregateFunctionExpr;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, InputOrderMode, PlanProperties,
};
use std::any::{type_name, Any};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

mod op;
mod stream;

pub use op::avg::avg;
pub use op::count::count;
pub use op::max::max;
pub use op::min::min;
pub use op::sum::sum;
pub(crate) use op::udf::CuDFAggregateUDF;
pub(crate) use op::CuDFAggregationOp;

/// GPU-accelerated GROUP BY aggregate execution node.
///
/// Replaces DataFusion's `AggregateExec` for queries where all aggregate
/// functions have cuDF implementations.
#[derive(Debug)]
pub struct CuDFAggregateExec {
    input: Arc<dyn ExecutionPlan>,
    mode: AggregateMode,
    group_by: PhysicalGroupBy,
    aggr_expr: Vec<Arc<AggregateFunctionExpr>>,

    plan_properties: Arc<PlanProperties>,
}

impl CuDFAggregateExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        mode: AggregateMode,
        group_by: PhysicalGroupBy,
        aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    ) -> Result<Self> {
        let input_schema = input.schema();

        // Non-single grouping sets (CUBE, ROLLUP) add an extra column for the grouping ID.
        let group_by_fields = {
            let num_exprs = group_by.expr().len();
            if !group_by.is_single() {
                num_exprs + 1
            } else {
                num_exprs
            }
        };

        let group_by_schema = group_by.group_schema(&input_schema)?;
        let group_by_exprs = group_by_schema.fields.iter().take(group_by_fields).cloned();

        let mut fields = Vec::with_capacity(group_by_fields + aggr_expr.len());

        fields.extend(group_by_exprs);

        // Partial mode emits intermediate state columns (e.g., AVG emits [count, sum]).
        // All other modes emit the final result column (e.g., AVG emits [avg]).
        if mode == AggregateMode::Partial {
            for expr in &aggr_expr {
                for field in expr.state_fields()? {
                    fields.push(field);
                }
            }
        } else {
            for expr in &aggr_expr {
                fields.push(expr.field());
            }
        }

        let output_schema = Arc::new(Schema::new_with_metadata(
            fields,
            input_schema.metadata.clone(),
        ));

        let group_by_expr_mapping =
            ProjectionMapping::try_new(group_by.expr().iter().cloned(), &input.schema())?;

        let plan_properties = Arc::new(AggregateExec::compute_properties(
            &input,
            output_schema,
            &group_by_expr_mapping,
            &mode,
            &InputOrderMode::Linear,
            &aggr_expr,
        )?);

        Ok(Self {
            input,
            mode,
            group_by,
            aggr_expr,
            plan_properties,
        })
    }
}

impl DisplayAs for CuDFAggregateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CuDFAggregateExec: ")?;
        write!(f, "mode={:?}, ", self.mode)?;
        write!(f, "group_by=[")?;
        for (i, (expr, alias)) in self.group_by.expr().iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}@{}", alias, expr)?;
        }
        write!(f, "], aggr_expr=[")?;
        for (i, expr) in self.aggr_expr.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", expr.name())?;
        }
        write!(f, "]")
    }
}

impl ExecutionPlan for CuDFAggregateExec {
    fn name(&self) -> &str {
        type_name::<Self>()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let new = Self::try_new(
            children[0].clone(),
            self.mode,
            self.group_by.clone(),
            self.aggr_expr.clone(),
        )?;

        Ok(Arc::new(new))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let stream = stream::Stream::new(
            input,
            self.schema(),
            self.mode,
            self.group_by.clone(),
            self.aggr_expr.clone(),
        )?;
        Ok(Box::pin(stream))
    }
}
#[cfg(test)]
mod test {
    use crate::aggregate::op::avg::avg;
    use crate::aggregate::op::count::count;
    use crate::aggregate::op::max::max;
    use crate::aggregate::op::min::min;
    use crate::aggregate::op::sum::sum;
    use crate::aggregate::CuDFAggregateExec;
    use crate::assert_snapshot;
    use crate::physical::{CuDFLoadExec, CuDFUnloadExec};
    use arrow::array::record_batch;
    use arrow::util::pretty::pretty_format_batches;
    use arrow_schema::SchemaRef;
    use datafusion::common::ScalarValue;
    use datafusion::execution::TaskContext;
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_expr::AggregateUDF;
    use datafusion_physical_plan::aggregates::{AggregateMode, PhysicalGroupBy};
    use datafusion_physical_plan::expressions::{col, Literal};
    use datafusion_physical_plan::test::TestMemoryExec;
    use datafusion_physical_plan::ExecutionPlan;
    use datafusion_physical_plan::PhysicalExpr;
    use futures_util::TryStreamExt;
    use std::error::Error;
    use std::sync::Arc;

    /// Run a GROUP BY aggregation through the full GPU pipeline:
    /// TestMemoryExec -> CuDFLoadExec -> CuDFAggregateExec -> CuDFUnloadExec.
    ///
    /// Sends 3 identical batches (to exercise cross-batch rolling merge) and
    /// groups by column "c". `build_args` receives the batch schema and returns
    /// the argument expressions for the aggregate function.
    async fn run_group_by_test(
        agg_fn: Arc<AggregateUDF>,
        build_args: impl FnOnce(&SchemaRef) -> datafusion::error::Result<Vec<Arc<dyn PhysicalExpr>>>,
        agg_alias: &str,
    ) -> Result<String, Box<dyn Error>> {
        let batch = record_batch!(
            ("a", Int64, [1, 4, 3]),
            ("b", Float64, [Some(4.0), None, Some(5.0)]),
            ("c", Utf8, ["hello", "hello", "world"]),
            ("d", Float64, [4.0, 5.0, 5.0])
        )
        .expect("created batch");

        let schema = batch.schema();

        let root = TestMemoryExec::try_new(
            &[vec![batch.clone(), batch.clone(), batch]],
            schema.clone(),
            None,
        )?;
        let load = CuDFLoadExec::try_new(Arc::new(root))?;

        let group_by = PhysicalGroupBy::new_single(vec![(col("c", &schema)?, "c".to_string())]);

        let agg = AggregateExprBuilder::new(agg_fn, build_args(&schema)?)
            .schema(schema)
            .alias(agg_alias)
            .build()?;

        let aggregate = CuDFAggregateExec::try_new(
            Arc::new(load),
            AggregateMode::Single,
            group_by,
            vec![Arc::new(agg)],
        )?;

        let unload = CuDFUnloadExec::new(Arc::new(aggregate));

        let task = Arc::new(TaskContext::default());

        let result = unload.execute(0, task)?;
        let batches = result.try_collect::<Vec<_>>().await?;

        let output = pretty_format_batches(&batches)?.to_string();
        Ok(output)
    }

    #[tokio::test]
    async fn test_group_by_sum() -> Result<(), Box<dyn Error>> {
        let output = run_group_by_test(sum(), |s| Ok(vec![col("a", s)?]), "SUM(a)").await?;

        // Note: cuDF's SUM always returns Int64 for integer inputs
        assert_snapshot!(output, @r"
        +-------+--------+
        | c     | SUM(a) |
        +-------+--------+
        | world | 9      |
        | hello | 15     |
        +-------+--------+
        ");

        Ok(())
    }

    #[tokio::test]
    async fn test_group_by_min() -> Result<(), Box<dyn Error>> {
        let output = run_group_by_test(min(), |s| Ok(vec![col("a", s)?]), "MIN(a)").await?;

        assert_snapshot!(output, @r"
        +-------+--------+
        | c     | MIN(a) |
        +-------+--------+
        | world | 3      |
        | hello | 1      |
        +-------+--------+
        ");

        Ok(())
    }

    #[tokio::test]
    async fn test_group_by_max() -> Result<(), Box<dyn Error>> {
        let output = run_group_by_test(max(), |s| Ok(vec![col("a", s)?]), "MAX(a)").await?;

        assert_snapshot!(output, @r"
        +-------+--------+
        | c     | MAX(a) |
        +-------+--------+
        | world | 3      |
        | hello | 4      |
        +-------+--------+
        ");

        Ok(())
    }

    #[tokio::test]
    async fn test_group_by_count() -> Result<(), Box<dyn Error>> {
        let output = run_group_by_test(count(), |s| Ok(vec![col("a", s)?]), "COUNT(a)").await?;

        assert_snapshot!(output, @r"
        +-------+----------+
        | c     | COUNT(a) |
        +-------+----------+
        | world | 3        |
        | hello | 6        |
        +-------+----------+
        ");

        Ok(())
    }

    /// COUNT with a literal argument — the exact code path hit by COUNT(*) = COUNT(lit(1)).
    #[tokio::test]
    async fn test_group_by_count_literal_arg() -> Result<(), Box<dyn Error>> {
        let lit_one: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int32(Some(1))));
        let output = run_group_by_test(count(), |_| Ok(vec![lit_one.clone()]), "COUNT(*)").await?;

        assert_snapshot!(output, @r"
        +-------+----------+
        | c     | COUNT(*) |
        +-------+----------+
        | world | 3        |
        | hello | 6        |
        +-------+----------+
        ");

        Ok(())
    }

    #[tokio::test]
    async fn test_group_by_avg() -> Result<(), Box<dyn Error>> {
        let output = run_group_by_test(avg(), |s| Ok(vec![col("a", s)?]), "AVG(a)").await?;

        assert_snapshot!(output, @r"
        +-------+--------+
        | c     | AVG(a) |
        +-------+--------+
        | world | 3.0    |
        | hello | 2.5    |
        +-------+--------+
        ");

        Ok(())
    }
}

/// Integration tests: full SQL pipeline through TestFramework against real weather data.
///
/// Tests using `check_query_results` omit ORDER BY — the helper sorts rows before
/// comparing, keeping plans free of sort operators. Float tests (AVG, mixed aggregates)
/// keep ORDER BY and use `assert_batches_approx_eq` to absorb last-ULP differences.
#[cfg(test)]
mod integration {
    use crate::assert_snapshot;
    use crate::test_utils::{check_query_results, sort_batches, TestFramework};
    use arrow::array::{Array, Float64Array, RecordBatch};
    use std::error::Error;

    /// Absorbs last-ULP differences between cuDF and DataFusion float arithmetic.
    /// Used only for tests that produce Float64 results (AVG, mixed aggregates).
    fn assert_batches_approx_eq(gpu: &[RecordBatch], cpu: &[RecordBatch], decimals: u32) {
        let factor = 10f64.powi(decimals as i32);
        assert_eq!(gpu.len(), cpu.len(), "batch count mismatch");
        for (b, (g, c)) in gpu.iter().zip(cpu.iter()).enumerate() {
            assert_eq!(g.num_rows(), c.num_rows(), "batch {b}: row count mismatch");
            assert_eq!(g.num_columns(), c.num_columns(), "batch {b}: column count");
            for col in 0..g.num_columns() {
                let gc = g.column(col);
                let cc = c.column(col);
                if let (Some(gf), Some(cf)) = (
                    gc.as_any().downcast_ref::<Float64Array>(),
                    cc.as_any().downcast_ref::<Float64Array>(),
                ) {
                    for row in 0..gf.len() {
                        let gv = (gf.value(row) * factor).round() / factor;
                        let cv = (cf.value(row) * factor).round() / factor;
                        assert_eq!(gv, cv, "batch {b}, col {col}, row {row}");
                    }
                } else {
                    assert_eq!(gc.as_ref(), cc.as_ref(), "batch {b}, col {col}");
                }
            }
        }
    }

    #[tokio::test]
    async fn test_sum() -> Result<(), Box<dyn Error>> {
        let sql = r#"SELECT "RainToday", SUM("Rainfall") as total_rain FROM weather GROUP BY "RainToday""#;
        let result = check_query_results(sql, 1).await?;
        assert_snapshot!(result.plan, @"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, sum(weather.Rainfall)@1 as total_rain]
            CuDFAggregateExec: mode=Final, group_by=[RainToday@RainToday@0], aggr_expr=[sum(weather.Rainfall)]
              CuDFLoadExec
                CoalesceBatchesExec: target_batch_size=81920
                  CoalescePartitionsExec
                    CuDFUnloadExec
                      CuDFCoalesceBatchesExec: target_batch_size=81920
                        CuDFAggregateExec: mode=Partial, group_by=[RainToday@RainToday@1], aggr_expr=[sum(weather.Rainfall)]
                          CuDFLoadExec
                            CoalesceBatchesExec: target_batch_size=81920
                              DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[Rainfall, RainToday], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_count() -> Result<(), Box<dyn Error>> {
        let sql = r#"SELECT "RainToday", COUNT("Rainfall") as n FROM weather GROUP BY "RainToday""#;
        let result = check_query_results(sql, 1).await?;
        assert_snapshot!(result.plan, @"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, count(weather.Rainfall)@1 as n]
            CuDFAggregateExec: mode=Final, group_by=[RainToday@RainToday@0], aggr_expr=[count(weather.Rainfall)]
              CuDFLoadExec
                CoalesceBatchesExec: target_batch_size=81920
                  CoalescePartitionsExec
                    CuDFUnloadExec
                      CuDFCoalesceBatchesExec: target_batch_size=81920
                        CuDFAggregateExec: mode=Partial, group_by=[RainToday@RainToday@1], aggr_expr=[count(weather.Rainfall)]
                          CuDFLoadExec
                            CoalesceBatchesExec: target_batch_size=81920
                              DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[Rainfall, RainToday], file_type=parquet
        ");
        Ok(())
    }

    /// AVG produces Float64 — uses assert_batches_approx_eq to handle last-ULP differences.
    #[tokio::test]
    async fn test_avg() -> Result<(), Box<dyn Error>> {
        let sql =
            r#"SELECT "RainToday", AVG("MinTemp") as avg_min FROM weather GROUP BY "RainToday""#;
        let tf = TestFramework::new().await;
        let gpu = tf
            .execute(&format!(
                "SET cudf.enable=true; SET datafusion.execution.target_partitions=1; {sql}"
            ))
            .await?;
        let cpu = tf
            .execute(&format!(
                "SET datafusion.execution.target_partitions=1; {sql}"
            ))
            .await?;
        assert_batches_approx_eq(&sort_batches(&gpu.batches), &sort_batches(&cpu.batches), 10);
        assert_snapshot!(gpu.plan, @"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, avg(weather.MinTemp)@1 as avg_min]
            CuDFAggregateExec: mode=Final, group_by=[RainToday@RainToday@0], aggr_expr=[avg(weather.MinTemp)]
              CuDFLoadExec
                CoalesceBatchesExec: target_batch_size=81920
                  CoalescePartitionsExec
                    CuDFUnloadExec
                      CuDFCoalesceBatchesExec: target_batch_size=81920
                        CuDFAggregateExec: mode=Partial, group_by=[RainToday@RainToday@1], aggr_expr=[avg(weather.MinTemp)]
                          CuDFLoadExec
                            CoalesceBatchesExec: target_batch_size=81920
                              DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_min_max() -> Result<(), Box<dyn Error>> {
        let sql = r#"SELECT "RainToday", MIN("MinTemp") as lo, MAX("MaxTemp") as hi FROM weather GROUP BY "RainToday""#;
        let result = check_query_results(sql, 1).await?;
        assert_snapshot!(result.plan, @"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, min(weather.MinTemp)@1 as lo, max(weather.MaxTemp)@2 as hi]
            CuDFAggregateExec: mode=Final, group_by=[RainToday@RainToday@0], aggr_expr=[min(weather.MinTemp), max(weather.MaxTemp)]
              CuDFLoadExec
                CoalesceBatchesExec: target_batch_size=81920
                  CoalescePartitionsExec
                    CuDFUnloadExec
                      CuDFCoalesceBatchesExec: target_batch_size=81920
                        CuDFAggregateExec: mode=Partial, group_by=[RainToday@RainToday@2], aggr_expr=[min(weather.MinTemp), max(weather.MaxTemp)]
                          CuDFLoadExec
                            CoalesceBatchesExec: target_batch_size=81920
                              DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, RainToday], file_type=parquet
        ");
        Ok(())
    }

    /// Contains AVG — uses assert_batches_approx_eq to handle last-ULP differences.
    #[tokio::test]
    async fn test_multiple_aggregates() -> Result<(), Box<dyn Error>> {
        let sql = r#"SELECT "RainToday", COUNT("Rainfall") as n, SUM("Rainfall") as total, AVG("MaxTemp") as avg_max, MIN("MinTemp") as lo, MAX("MaxTemp") as hi FROM weather GROUP BY "RainToday""#;
        let tf = TestFramework::new().await;
        let gpu = tf
            .execute(&format!(
                "SET cudf.enable=true; SET datafusion.execution.target_partitions=1; {sql}"
            ))
            .await?;
        let cpu = tf
            .execute(&format!(
                "SET datafusion.execution.target_partitions=1; {sql}"
            ))
            .await?;
        assert_batches_approx_eq(&sort_batches(&gpu.batches), &sort_batches(&cpu.batches), 10);
        assert_snapshot!(gpu.plan, @"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, count(weather.Rainfall)@1 as n, sum(weather.Rainfall)@2 as total, avg(weather.MaxTemp)@3 as avg_max, min(weather.MinTemp)@4 as lo, max(weather.MaxTemp)@5 as hi]
            CuDFAggregateExec: mode=Final, group_by=[RainToday@RainToday@0], aggr_expr=[count(weather.Rainfall), sum(weather.Rainfall), avg(weather.MaxTemp), min(weather.MinTemp), max(weather.MaxTemp)]
              CuDFLoadExec
                CoalesceBatchesExec: target_batch_size=81920
                  CoalescePartitionsExec
                    CuDFUnloadExec
                      CuDFCoalesceBatchesExec: target_batch_size=81920
                        CuDFAggregateExec: mode=Partial, group_by=[RainToday@RainToday@3], aggr_expr=[count(weather.Rainfall), sum(weather.Rainfall), avg(weather.MaxTemp), min(weather.MinTemp), max(weather.MaxTemp)]
                          CuDFLoadExec
                            CoalesceBatchesExec: target_batch_size=81920
                              DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, Rainfall, RainToday], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_count_star() -> Result<(), Box<dyn Error>> {
        let sql = r#"SELECT "RainToday", COUNT(*) as n FROM weather GROUP BY "RainToday""#;
        let result = check_query_results(sql, 1).await?;
        assert_snapshot!(result.plan, @"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, count(Int64(1))@1 as n]
            CuDFAggregateExec: mode=Final, group_by=[RainToday@RainToday@0], aggr_expr=[count(Int64(1))]
              CuDFLoadExec
                CoalesceBatchesExec: target_batch_size=81920
                  CoalescePartitionsExec
                    CuDFUnloadExec
                      CuDFCoalesceBatchesExec: target_batch_size=81920
                        CuDFAggregateExec: mode=Partial, group_by=[RainToday@RainToday@0], aggr_expr=[count(Int64(1))]
                          CuDFLoadExec
                            CoalesceBatchesExec: target_batch_size=81920
                              DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_count_star_mixed() -> Result<(), Box<dyn Error>> {
        let sql = r#"SELECT "RainToday", COUNT(*) as n, SUM("Rainfall") as total FROM weather GROUP BY "RainToday""#;
        let result = check_query_results(sql, 1).await?;
        assert_snapshot!(result.plan, @"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, count(Int64(1))@1 as n, sum(weather.Rainfall)@2 as total]
            CuDFAggregateExec: mode=Final, group_by=[RainToday@RainToday@0], aggr_expr=[count(Int64(1)), sum(weather.Rainfall)]
              CuDFLoadExec
                CoalesceBatchesExec: target_batch_size=81920
                  CoalescePartitionsExec
                    CuDFUnloadExec
                      CuDFCoalesceBatchesExec: target_batch_size=81920
                        CuDFAggregateExec: mode=Partial, group_by=[RainToday@RainToday@1], aggr_expr=[count(Int64(1)), sum(weather.Rainfall)]
                          CuDFLoadExec
                            CoalesceBatchesExec: target_batch_size=81920
                              DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[Rainfall, RainToday], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_multi_partition_sum() -> Result<(), Box<dyn Error>> {
        let sql =
            r#"SELECT "RainToday", SUM("Rainfall") as total FROM weather GROUP BY "RainToday""#;
        let result = check_query_results(sql, 4).await?;
        assert_snapshot!(result.plan, @"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, sum(weather.Rainfall)@1 as total]
            CuDFAggregateExec: mode=FinalPartitioned, group_by=[RainToday@RainToday@0], aggr_expr=[sum(weather.Rainfall)]
              CuDFLoadExec
                CoalesceBatchesExec: target_batch_size=81920
                  RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=3
                    CuDFUnloadExec
                      CuDFCoalesceBatchesExec: target_batch_size=81920
                        CuDFAggregateExec: mode=Partial, group_by=[RainToday@RainToday@1], aggr_expr=[sum(weather.Rainfall)]
                          CuDFLoadExec
                            CoalesceBatchesExec: target_batch_size=81920
                              DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[Rainfall, RainToday], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_multi_partition_count_star() -> Result<(), Box<dyn Error>> {
        let sql = r#"SELECT "RainToday", COUNT(*) as n FROM weather GROUP BY "RainToday""#;
        let result = check_query_results(sql, 4).await?;
        assert_snapshot!(result.plan, @"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, count(Int64(1))@1 as n]
            CuDFAggregateExec: mode=FinalPartitioned, group_by=[RainToday@RainToday@0], aggr_expr=[count(Int64(1))]
              CuDFLoadExec
                CoalesceBatchesExec: target_batch_size=81920
                  RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=3
                    CuDFUnloadExec
                      CuDFCoalesceBatchesExec: target_batch_size=81920
                        CuDFAggregateExec: mode=Partial, group_by=[RainToday@RainToday@0], aggr_expr=[count(Int64(1))]
                          CuDFLoadExec
                            CoalesceBatchesExec: target_batch_size=81920
                              DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
        ");
        Ok(())
    }

    /// Contains AVG — uses assert_batches_approx_eq to handle last-ULP differences.
    #[tokio::test]
    async fn test_multi_partition_multiple_aggs() -> Result<(), Box<dyn Error>> {
        let sql = r#"SELECT "RainToday", COUNT(*) as n, SUM("Rainfall") as total, AVG("MaxTemp") as avg_max, MIN("MinTemp") as lo, MAX("MaxTemp") as hi FROM weather GROUP BY "RainToday""#;
        let tf = TestFramework::new().await;
        let gpu = tf
            .execute(&format!(
                "SET cudf.enable=true; SET datafusion.execution.target_partitions=4; {sql}"
            ))
            .await?;
        let cpu = tf
            .execute(&format!(
                "SET datafusion.execution.target_partitions=4; {sql}"
            ))
            .await?;
        assert_batches_approx_eq(&sort_batches(&gpu.batches), &sort_batches(&cpu.batches), 10);
        assert_snapshot!(gpu.plan, @"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, count(Int64(1))@1 as n, sum(weather.Rainfall)@2 as total, avg(weather.MaxTemp)@3 as avg_max, min(weather.MinTemp)@4 as lo, max(weather.MaxTemp)@5 as hi]
            CuDFAggregateExec: mode=FinalPartitioned, group_by=[RainToday@RainToday@0], aggr_expr=[count(Int64(1)), sum(weather.Rainfall), avg(weather.MaxTemp), min(weather.MinTemp), max(weather.MaxTemp)]
              CuDFLoadExec
                CoalesceBatchesExec: target_batch_size=81920
                  RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=3
                    CuDFUnloadExec
                      CuDFCoalesceBatchesExec: target_batch_size=81920
                        CuDFAggregateExec: mode=Partial, group_by=[RainToday@RainToday@3], aggr_expr=[count(Int64(1)), sum(weather.Rainfall), avg(weather.MaxTemp), min(weather.MinTemp), max(weather.MaxTemp)]
                          CuDFLoadExec
                            CoalesceBatchesExec: target_batch_size=81920
                              DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, Rainfall, RainToday], file_type=parquet
        ");
        Ok(())
    }

    /// Aggregates with unsupported functions (non-CuDFAggregateUDF) must fall back to CPU.
    #[tokio::test]
    async fn test_unsupported_agg_falls_back_to_cpu() -> Result<(), Box<dyn Error>> {
        let tf = TestFramework::new().await;
        let sql = r#"SELECT "RainToday", BOOL_OR("RainTomorrow" = 'Yes') as any_rain FROM weather GROUP BY "RainToday""#;
        let gpu = tf.execute(&format!("SET cudf.enable=true; {sql}")).await?;
        let cpu = tf.execute(sql).await?;
        assert!(
            !gpu.plan.contains("CuDFAggregateExec"),
            "expected CPU fallback"
        );
        assert_eq!(cpu.pretty_print, gpu.pretty_print);
        Ok(())
    }
}
