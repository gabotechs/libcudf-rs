use crate::expr::expr_to_cudf_expr;
use datafusion::common::Statistics;
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::execution_plan::CardinalityEffect;
use datafusion_physical_plan::filter_pushdown::{FilterDescription, FilterPushdownPhase};
use datafusion_physical_plan::metrics::MetricsSet;
use datafusion_physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
};
use delegate::delegate;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

#[derive(Debug)]
pub struct CuDFProjectionExec {
    host_exec: ProjectionExec,
    cudf_exec: ProjectionExec,
}

impl CuDFProjectionExec {
    pub fn try_new(host_exec: ProjectionExec) -> Result<Self, DataFusionError> {
        let original_expressions = host_exec.expr();
        let mut expressions = Vec::with_capacity(original_expressions.len());
        for expr in original_expressions {
            expressions.push(ProjectionExpr {
                alias: expr.alias.to_string(),
                expr: expr_to_cudf_expr(expr.expr.as_ref())?,
            })
        }

        let cudf_exec = ProjectionExec::try_new(expressions, Arc::clone(host_exec.input()))?;
        Ok(Self {
            host_exec,
            cudf_exec,
        })
    }
}

impl DisplayAs for CuDFProjectionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CuDF")?;
        self.host_exec.fmt_as(t, f)
    }
}

impl ExecutionPlan for CuDFProjectionExec {
    fn name(&self) -> &str {
        "CuDFProjectionExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let p_exe = ProjectionExec::try_new(
            self.host_exec.expr().iter().cloned(),
            children.swap_remove(0),
        )?;
        Ok(Arc::new(Self::try_new(p_exe)?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        self.cudf_exec.execute(partition, context)
    }

    delegate! {
        to self.host_exec {
            fn properties(&self) -> &PlanProperties;
            fn maintains_input_order(&self) -> Vec<bool>;
            fn benefits_from_input_partitioning(&self) -> Vec<bool>;
            fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>>;
            fn cardinality_effect(&self) -> CardinalityEffect;
            fn supports_limit_pushdown(&self) -> bool;
            fn gather_filters_for_pushdown(&self, phase: FilterPushdownPhase, parent_filters: Vec<Arc<dyn PhysicalExpr>>, config: &ConfigOptions) -> Result<FilterDescription, DataFusionError>;
        }
    }

    delegate! {
        to self.cudf_exec {
            fn metrics(&self) -> Option<MetricsSet>;
            fn partition_statistics(&self, partition: Option<usize>) -> datafusion::common::Result<Statistics>;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_snapshot;
    use crate::test_utils::TestFramework;

    #[tokio::test]
    async fn test_basic_projection() -> Result<(), DataFusionError> {
        let tf = TestFramework::new().await;

        let plan = tf
            .sql(
                r#"
            SET datafusion.execution.target_partitions=1;
            SET cudf.enable=true;
            SELECT "MinTemp" + 1 FROM weather LIMIT 1"#,
            )
            .await?;

        assert_snapshot!(plan.display(), @r"
        CudfUnloadExec
          CuDFProjectionExec: expr=[MinTemp@0 + 1 as weather.MinTemp + Int64(1)]
            CudfLoadExec
              DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000002.parquet]]}, projection=[MinTemp], limit=1, file_type=parquet
        ");
        let result = plan.execute().await?;
        assert_snapshot!(result.pretty_print, @r"
        +----------------------------+
        | weather.MinTemp + Int64(1) |
        +----------------------------+
        | 13.2                       |
        +----------------------------+
        ");
        Ok(())
    }
}
