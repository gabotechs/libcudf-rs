use crate::expr::expr_to_cudf_expr;
use crate::physical::aggregate::op::count::CuDFCount;
use crate::planner::CuDFConfig;
use arrow_schema::{DataType, Schema, SchemaRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::projection::ProjectionMapping;
use datafusion::physical_expr_common::metrics::MetricsSet;
use datafusion_physical_plan::aggregates::{
    aggregate_expressions, AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion_physical_plan::expressions::{Column, Literal};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_plan::udaf::AggregateFunctionExpr;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, InputOrderMode, PhysicalExpr, PlanProperties,
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

/// A fully validated cuDF aggregate plan for one DataFusion `AggregateExec`.
#[derive(Debug, Clone)]
pub(crate) struct PreparedCuDFAggregate {
    mode: AggregateMode,
    group_by: PhysicalGroupBy,
    aggs: Vec<PreparedAggregate>,
}

/// One aggregate expression with all cuDF execution details resolved.
#[derive(Debug, Clone)]
pub(crate) struct PreparedAggregate {
    expr: Arc<AggregateFunctionExpr>,
    op: Arc<dyn CuDFAggregationOp>,
    args: Vec<Arc<dyn PhysicalExpr>>,
    output_type: DataType,
}

impl PreparedCuDFAggregate {
    fn aggr_expr(&self) -> Vec<Arc<AggregateFunctionExpr>> {
        self.aggs.iter().map(|agg| agg.expr.clone()).collect()
    }
}

/// GPU-accelerated GROUP BY aggregate execution node.
///
/// Replaces DataFusion's `AggregateExec` for queries where all aggregate
/// functions have cuDF implementations.
#[derive(Debug)]
pub struct CuDFAggregateExec {
    input: Arc<dyn ExecutionPlan>,
    prepared: PreparedCuDFAggregate,
    plan_properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl CuDFAggregateExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        mode: AggregateMode,
        group_by: PhysicalGroupBy,
        aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    ) -> Result<Self> {
        let input_schema = input.schema();
        let Some(prepared) =
            prepare_cudf_aggregate_parts(mode, group_by, aggr_expr, &input_schema)?
        else {
            return Err(datafusion::error::DataFusionError::NotImplemented(
                "Aggregate is not supported by cuDF".to_string(),
            ));
        };

        Self::try_new_prepared(input, prepared)
    }

    fn try_new_prepared(
        input: Arc<dyn ExecutionPlan>,
        prepared: PreparedCuDFAggregate,
    ) -> Result<Self> {
        let input_schema = input.schema();
        let mode = prepared.mode;
        let aggr_expr = prepared.aggr_expr();

        let group_by_fields = prepared.group_by.expr().len();

        let group_by_schema = prepared.group_by.group_schema(&input_schema)?;
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
            ProjectionMapping::try_new(prepared.group_by.expr().iter().cloned(), &input.schema())?;

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
            prepared,
            plan_properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl DisplayAs for CuDFAggregateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CuDFAggregateExec: ")?;
        write!(f, "mode={:?}, ", self.prepared.mode)?;
        write!(f, "group_by=[")?;
        for (i, (expr, alias)) in self.prepared.group_by.expr().iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}@{}", alias, expr)?;
        }
        write!(f, "], aggr_expr=[")?;
        for (i, agg) in self.prepared.aggs.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", agg.expr.name())?;
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
            self.prepared.mode,
            self.prepared.group_by.clone(),
            self.prepared.aggr_expr(),
        )?;

        Ok(Arc::new(new))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let cudf_cfg = CuDFConfig::from_config_options(context.session_config().options())?;
        let aggregate_chunk_target_bytes = cudf_cfg.aggregate_chunk_target_bytes;
        let input = self.input.execute(partition, context)?;
        let stream = stream::CuDFAggregateStream::new(
            input,
            self.schema(),
            self.prepared.clone(),
            aggregate_chunk_target_bytes,
            &self.metrics,
            partition,
        )?;
        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

fn prepare_cudf_aggregate(node: &AggregateExec) -> Result<Option<PreparedCuDFAggregate>> {
    prepare_cudf_aggregate_parts(
        *node.mode(),
        node.group_expr().clone(),
        node.aggr_expr().to_vec(),
        &node.input().schema(),
    )
}

fn prepare_cudf_aggregate_parts(
    mode: AggregateMode,
    group_by: PhysicalGroupBy,
    aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    input_schema: &SchemaRef,
) -> Result<Option<PreparedCuDFAggregate>> {
    if group_by.expr().is_empty() {
        return Ok(None);
    }
    if !group_by.is_single() {
        return Ok(None);
    }
    for (expr, _) in group_by.expr() {
        if expr.as_any().downcast_ref::<Column>().is_none() {
            return Ok(None);
        }
    }
    for expr in &aggr_expr {
        if expr.is_distinct() || !expr.order_bys().is_empty() {
            return Ok(None);
        }
    }

    let aggregate_args = aggregate_expressions(&aggr_expr, &mode, group_by.expr().len())?;

    let mut aggs = Vec::with_capacity(aggregate_args.len());
    for (expr, args) in aggr_expr.into_iter().zip(aggregate_args) {
        let Some(udf) = expr
            .fun()
            .inner()
            .as_any()
            .downcast_ref::<CuDFAggregateUDF>()
        else {
            return Ok(None);
        };
        let mut op = udf.gpu().clone();

        if !original_aggregate_args_supported(&expr)? {
            return Ok(None);
        }

        let output_type = expr.field().data_type().clone();
        let arg_types = args
            .iter()
            .map(|arg| arg.data_type(input_schema))
            .collect::<Result<Vec<DataType>>>()?;
        if !op.supports_input_types(mode, &arg_types, &output_type) {
            return Ok(None);
        }

        let count_star = is_count_star(&expr);
        let mut converted = Vec::with_capacity(args.len());
        for arg in args {
            let Some(arg) = expr_to_cudf_aggregate_arg(arg)? else {
                return Ok(None);
            };
            converted.push(arg);
        }
        if count_star && !matches!(mode, AggregateMode::Final | AggregateMode::FinalPartitioned) {
            let Some((arg, _)) = group_by.expr().first() else {
                return Ok(None);
            };
            op = Arc::new(CuDFCount::count_all());
            converted = vec![arg.clone()];
        }
        aggs.push(PreparedAggregate {
            expr,
            op,
            args: converted,
            output_type,
        });
    }

    Ok(Some(PreparedCuDFAggregate {
        mode,
        group_by,
        aggs,
    }))
}

fn is_count_star(expr: &AggregateFunctionExpr) -> bool {
    expr.name().starts_with("count(")
        && expr.expressions().iter().all(|arg| {
            arg.as_any()
                .downcast_ref::<Literal>()
                .is_some_and(|literal| !literal.value().is_null())
        })
}

fn original_aggregate_args_supported(expr: &AggregateFunctionExpr) -> Result<bool> {
    // Final aggregates see only state columns, so they may look GPU-compatible
    // even when the matching partial aggregate had unsupported raw arguments.
    // Keep the whole aggregate chain on one side of the CPU/GPU boundary.
    for arg in expr.expressions() {
        if expr_to_cudf_aggregate_arg(arg)?.is_none() {
            return Ok(false);
        }
    }
    Ok(true)
}

fn expr_to_cudf_aggregate_arg(arg: Arc<dyn PhysicalExpr>) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    // A bare literal aggregate argument, e.g. COUNT(*), must keep DataFusion's
    // normal batch-length expansion. CuDFLiteral evaluates to a scalar, which is
    // correct inside binary GPU expressions but not as a direct aggregate input.
    if arg.as_any().downcast_ref::<Literal>().is_some() {
        return Ok(Some(arg));
    }

    match expr_to_cudf_expr(arg.as_ref()) {
        Ok(expr) => Ok(Some(expr)),
        Err(DataFusionError::NotImplemented(_)) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Try to convert an `AggregateExec` to a `CuDFAggregateExec`.
///
/// Returns `Ok(None)` if any part of the aggregate is not supported by the GPU
/// implementation so the planner can keep the original CPU aggregate.
pub(crate) fn try_as_cudf_aggregate(
    node: &AggregateExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let Some(prepared) = prepare_cudf_aggregate(node)? else {
        return Ok(None);
    };
    Ok(Some(Arc::new(CuDFAggregateExec::try_new_prepared(
        node.input().clone(),
        prepared,
    )?)))
}

#[cfg(test)]
mod test {
    use crate::assert_snapshot;
    use crate::physical::aggregate::op::avg::avg;
    use crate::physical::aggregate::op::count::count;
    use crate::physical::aggregate::op::max::max;
    use crate::physical::aggregate::op::min::min;
    use crate::physical::aggregate::op::sum::sum;
    use crate::physical::aggregate::CuDFAggregateExec;
    use crate::physical::{CuDFLoadExec, CuDFUnloadExec};
    use crate::test_utils::sort_batches;
    use crate::CuDFConfig;
    use arrow::array::{record_batch, Array, Int64Array, StringArray, StringViewArray};
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::pretty_format_batches;
    use arrow_schema::SchemaRef;
    use datafusion::common::ScalarValue;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::execution::TaskContext;
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::prelude::SessionConfig;
    use datafusion_expr::AggregateUDF;
    use datafusion_physical_plan::aggregates::{AggregateMode, PhysicalGroupBy};
    use datafusion_physical_plan::expressions::{col, Literal};
    use datafusion_physical_plan::test::TestMemoryExec;
    use datafusion_physical_plan::ExecutionPlan;
    use datafusion_physical_plan::PhysicalExpr;
    use futures_util::TryStreamExt;
    use std::collections::HashMap;
    use std::error::Error;
    use std::sync::Arc;

    async fn run_group_by_test(
        agg_fn: Arc<AggregateUDF>,
        build_args: impl FnOnce(&SchemaRef) -> datafusion::error::Result<Vec<Arc<dyn PhysicalExpr>>>,
        agg_alias: &str,
    ) -> Result<String, Box<dyn Error>> {
        let batches = collect_group_by_test(
            agg_fn,
            build_args,
            agg_alias,
            Arc::new(TaskContext::default().with_session_config(
                SessionConfig::default().with_option_extension(CuDFConfig::default()),
            )),
        )
        .await?;

        let sorted = sort_batches(&batches);
        let output = pretty_format_batches(&sorted)?.to_string();
        Ok(output)
    }

    async fn collect_group_by_test(
        agg_fn: Arc<AggregateUDF>,
        build_args: impl FnOnce(&SchemaRef) -> datafusion::error::Result<Vec<Arc<dyn PhysicalExpr>>>,
        agg_alias: &str,
        task: Arc<TaskContext>,
    ) -> Result<Vec<RecordBatch>, Box<dyn Error>> {
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

        let result = unload.execute(0, task)?;
        let batches = result.try_collect::<Vec<_>>().await?;

        Ok(batches)
    }

    #[tokio::test]
    async fn test_group_by_sum() -> Result<(), Box<dyn Error>> {
        let output = run_group_by_test(sum(), |s| Ok(vec![col("a", s)?]), "SUM(a)").await?;

        // Note: cuDF's SUM always returns Int64 for integer inputs
        assert_snapshot!(output, @r"
        +-------+--------+
        | c     | SUM(a) |
        +-------+--------+
        | hello | 15     |
        | world | 9      |
        +-------+--------+
        ");

        Ok(())
    }

    #[tokio::test]
    async fn test_group_by_sum_with_tiny_aggregate_chunk() -> Result<(), Box<dyn Error>> {
        let batches = collect_group_by_test(
            sum(),
            |s| Ok(vec![col("a", s)?]),
            "SUM(a)",
            task_ctx_with_aggregate_chunk_target_bytes(1),
        )
        .await?;

        let mut rows = grouped_i64_rows(&batches);
        rows.sort();
        assert_eq!(
            rows,
            vec![("hello".to_string(), 15), ("world".to_string(), 9)]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_group_by_min() -> Result<(), Box<dyn Error>> {
        let output = run_group_by_test(min(), |s| Ok(vec![col("a", s)?]), "MIN(a)").await?;

        assert_snapshot!(output, @r"
        +-------+--------+
        | c     | MIN(a) |
        +-------+--------+
        | hello | 1      |
        | world | 3      |
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
        | hello | 4      |
        | world | 3      |
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
        | hello | 6        |
        | world | 3        |
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
        | hello | 6        |
        | world | 3        |
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
        | hello | 2.5    |
        | world | 3.0    |
        +-------+--------+
        ");

        Ok(())
    }

    fn task_ctx_with_aggregate_chunk_target_bytes(bytes: usize) -> Arc<TaskContext> {
        let cudf_config = CuDFConfig {
            aggregate_chunk_target_bytes: bytes,
            ..CuDFConfig::default()
        };
        let session_config = SessionConfig::new().with_option_extension(cudf_config);

        Arc::new(TaskContext::new(
            None,
            "aggregate-test".to_string(),
            session_config,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            Arc::new(RuntimeEnv::default()),
        ))
    }

    fn grouped_i64_rows(batches: &[RecordBatch]) -> Vec<(String, i64)> {
        assert_eq!(batches.len(), 1, "expected one aggregate output batch");
        let batch = &batches[0];
        let sums = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("expected int64 sums");

        (0..batch.num_rows())
            .map(|i| (string_value(batch.column(0).as_ref(), i), sums.value(i)))
            .collect()
    }

    fn string_value(array: &dyn Array, row: usize) -> String {
        if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
            return array.value(row).to_string();
        }
        if let Some(array) = array.as_any().downcast_ref::<StringViewArray>() {
            return array.value(row).to_string();
        }
        panic!("expected string group keys, got {}", array.data_type());
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
        assert_snapshot!(result.plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, sum(weather.Rainfall)@1 as total_rain]
            CuDFAggregateExec: mode=Single, group_by=[RainToday@RainToday@1], aggr_expr=[sum(weather.Rainfall)]
              CuDFLoadExec
                DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[Rainfall, RainToday], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_count() -> Result<(), Box<dyn Error>> {
        let sql = r#"SELECT "RainToday", COUNT("Rainfall") as n FROM weather GROUP BY "RainToday""#;
        let result = check_query_results(sql, 1).await?;
        assert_snapshot!(result.plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, count(weather.Rainfall)@1 as n]
            CuDFAggregateExec: mode=Single, group_by=[RainToday@RainToday@1], aggr_expr=[count(weather.Rainfall)]
              CuDFLoadExec
                DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[Rainfall, RainToday], file_type=parquet
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
        assert_snapshot!(gpu.plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, avg(weather.MinTemp)@1 as avg_min]
            CuDFAggregateExec: mode=Single, group_by=[RainToday@RainToday@1], aggr_expr=[avg(weather.MinTemp)]
              CuDFLoadExec
                DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_min_max() -> Result<(), Box<dyn Error>> {
        let sql = r#"SELECT "RainToday", MIN("MinTemp") as lo, MAX("MaxTemp") as hi FROM weather GROUP BY "RainToday""#;
        let result = check_query_results(sql, 1).await?;
        assert_snapshot!(result.plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, min(weather.MinTemp)@1 as lo, max(weather.MaxTemp)@2 as hi]
            CuDFAggregateExec: mode=Single, group_by=[RainToday@RainToday@2], aggr_expr=[min(weather.MinTemp), max(weather.MaxTemp)]
              CuDFLoadExec
                DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, RainToday], file_type=parquet
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
        assert_snapshot!(gpu.plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, count(weather.Rainfall)@1 as n, sum(weather.Rainfall)@2 as total, avg(weather.MaxTemp)@3 as avg_max, min(weather.MinTemp)@4 as lo, max(weather.MaxTemp)@5 as hi]
            CuDFAggregateExec: mode=Single, group_by=[RainToday@RainToday@3], aggr_expr=[count(weather.Rainfall), sum(weather.Rainfall), avg(weather.MaxTemp), min(weather.MinTemp), max(weather.MaxTemp)]
              CuDFLoadExec
                DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, Rainfall, RainToday], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_count_star() -> Result<(), Box<dyn Error>> {
        let sql = r#"SELECT "RainToday", COUNT(*) as n FROM weather GROUP BY "RainToday""#;
        let result = check_query_results(sql, 1).await?;
        assert_snapshot!(result.plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, count(Int64(1))@1 as n]
            CuDFAggregateExec: mode=Single, group_by=[RainToday@RainToday@0], aggr_expr=[count(Int64(1))]
              CuDFLoadExec
                DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_count_star_mixed() -> Result<(), Box<dyn Error>> {
        let sql = r#"SELECT "RainToday", COUNT(*) as n, SUM("Rainfall") as total FROM weather GROUP BY "RainToday""#;
        let result = check_query_results(sql, 1).await?;
        assert_snapshot!(result.plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, count(Int64(1))@1 as n, sum(weather.Rainfall)@2 as total]
            CuDFAggregateExec: mode=Single, group_by=[RainToday@RainToday@1], aggr_expr=[count(Int64(1)), sum(weather.Rainfall)]
              CuDFLoadExec
                DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[Rainfall, RainToday], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_multi_partition_sum() -> Result<(), Box<dyn Error>> {
        let sql =
            r#"SELECT "RainToday", SUM("Rainfall") as total FROM weather GROUP BY "RainToday""#;
        let result = check_query_results(sql, 4).await?;
        assert_snapshot!(result.plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, sum(weather.Rainfall)@1 as total]
            CuDFAggregateExec: mode=FinalPartitioned, group_by=[RainToday@RainToday@0], aggr_expr=[sum(weather.Rainfall)]
              CuDFLoadExec
                RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=1
                  CuDFUnloadExec
                    CuDFAggregateExec: mode=Partial, group_by=[RainToday@RainToday@1], aggr_expr=[sum(weather.Rainfall)]
                      CuDFLoadExec
                        DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[Rainfall, RainToday], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_multi_partition_count_star() -> Result<(), Box<dyn Error>> {
        let sql = r#"SELECT "RainToday", COUNT(*) as n FROM weather GROUP BY "RainToday""#;
        let result = check_query_results(sql, 4).await?;
        assert_snapshot!(result.plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, count(Int64(1))@1 as n]
            CuDFAggregateExec: mode=FinalPartitioned, group_by=[RainToday@RainToday@0], aggr_expr=[count(Int64(1))]
              CuDFLoadExec
                RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=1
                  CuDFUnloadExec
                    CuDFAggregateExec: mode=Partial, group_by=[RainToday@RainToday@0], aggr_expr=[count(Int64(1))]
                      CuDFLoadExec
                        DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
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
        assert_snapshot!(gpu.plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, count(Int64(1))@1 as n, sum(weather.Rainfall)@2 as total, avg(weather.MaxTemp)@3 as avg_max, min(weather.MinTemp)@4 as lo, max(weather.MaxTemp)@5 as hi]
            CuDFAggregateExec: mode=FinalPartitioned, group_by=[RainToday@RainToday@0], aggr_expr=[count(Int64(1)), sum(weather.Rainfall), avg(weather.MaxTemp), min(weather.MinTemp), max(weather.MaxTemp)]
              CuDFLoadExec
                RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=1
                  CuDFUnloadExec
                    CuDFAggregateExec: mode=Partial, group_by=[RainToday@RainToday@3], aggr_expr=[count(Int64(1)), sum(weather.Rainfall), avg(weather.MaxTemp), min(weather.MinTemp), max(weather.MaxTemp)]
                      CuDFLoadExec
                        DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, Rainfall, RainToday], file_type=parquet
        ");
        Ok(())
    }

    /// Aggregates with unsupported functions (non-CuDFAggregateUDF) must fall back to CPU.
    #[tokio::test]
    async fn test_unsupported_agg_falls_back_to_cpu() -> Result<(), Box<dyn Error>> {
        let tf = TestFramework::new().await;
        let sql = r#"SELECT "RainToday", BOOL_OR("RainTomorrow" = 'Yes') as any_rain FROM weather GROUP BY "RainToday" ORDER BY "RainToday""#;
        let gpu = tf.execute(&format!("SET cudf.enable=true; {sql}")).await?;
        let cpu = tf.execute(sql).await?;
        assert!(
            !gpu.plan.contains("CuDFAggregateExec"),
            "expected CPU fallback"
        );
        assert_eq!(cpu.pretty_print, gpu.pretty_print);
        Ok(())
    }

    /// Aggregates with unsupported argument expressions must also fall back to CPU.
    #[tokio::test]
    async fn test_unsupported_agg_arg_falls_back_to_cpu() -> Result<(), Box<dyn Error>> {
        let tf = TestFramework::new().await;
        let sql = r#"SELECT "RainToday", COUNT(SUBSTR("RainTomorrow", 1, 1)) as n FROM weather GROUP BY "RainToday" ORDER BY "RainToday""#;
        let gpu = tf.execute(&format!("SET cudf.enable=true; {sql}")).await?;
        let cpu = tf.execute(sql).await?;
        assert!(
            !gpu.plan.contains("CuDFAggregateExec"),
            "unsupported aggregate argument should keep AggregateExec:\n{}",
            gpu.plan
        );
        assert_eq!(cpu.pretty_print, gpu.pretty_print);
        Ok(())
    }

    #[tokio::test]
    async fn test_decimal_sum_expression_uses_cudf_args() -> Result<(), Box<dyn Error>> {
        let tf = TestFramework::new().await;
        tf.execute(
            r#"CREATE TABLE decimal_sales (
                k VARCHAR,
                price DECIMAL(10, 2),
                discount DECIMAL(10, 2)
            ) AS VALUES
                ('a', 10.00, 1.25),
                ('a', 20.00, 2.50),
                ('b', 30.00, 3.75)"#,
        )
        .await?;

        let sql = "SELECT k, SUM(price - discount) AS net FROM decimal_sales GROUP BY k ORDER BY k";
        let gpu = tf.execute(&format!("SET cudf.enable=true; {sql}")).await?;
        let cpu = tf.execute(sql).await?;
        assert!(
            gpu.plan.contains("CuDFAggregateExec"),
            "expected decimal aggregate expression to use cuDF:\n{}",
            gpu.plan
        );
        assert_eq!(cpu.pretty_print, gpu.pretty_print);
        Ok(())
    }

    #[tokio::test]
    async fn test_decimal_avg_uses_cudf() -> Result<(), Box<dyn Error>> {
        let tf = TestFramework::new().await;
        tf.execute(
            r#"CREATE TABLE decimal_sales (
                k VARCHAR,
                price DECIMAL(12, 4)
            ) AS VALUES
                ('a', 10.0100),
                ('a', 20.0200),
                ('b', 30.0300)"#,
        )
        .await?;

        let sql = "SELECT k, AVG(price) AS avg_price FROM decimal_sales GROUP BY k ORDER BY k";
        let gpu = tf.execute(&format!("SET cudf.enable=true; {sql}")).await?;
        let cpu = tf.execute(sql).await?;
        assert!(
            gpu.plan.contains("CuDFAggregateExec"),
            "expected AVG(decimal) to use cuDF:\n{}",
            gpu.plan
        );
        assert_eq!(cpu.pretty_print, gpu.pretty_print);
        Ok(())
    }
}
