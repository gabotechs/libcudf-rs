use crate::expr::expr_to_cudf_expr;
use datafusion::common::Statistics;
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::execution_plan::CardinalityEffect;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::filter_pushdown::{FilterDescription, FilterPushdownPhase};
use datafusion_physical_plan::metrics::MetricsSet;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
};
use delegate::delegate;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

#[derive(Debug)]
pub struct CuDFFilterExec {
    host_exec: FilterExec,
    cudf_exec: FilterExec,
}

impl CuDFFilterExec {
    pub fn try_new(host_exec: FilterExec) -> Result<Self, DataFusionError> {
        let predicate = expr_to_cudf_expr(host_exec.predicate().as_ref())?;
        let cudf_exec = FilterExec::try_new(predicate, Arc::clone(host_exec.input()))?;
        Ok(Self {
            host_exec,
            cudf_exec,
        })
    }
}

impl DisplayAs for CuDFFilterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CuDF")?;
        self.host_exec.fmt_as(t, f)
    }
}

impl ExecutionPlan for CuDFFilterExec {
    fn name(&self) -> &str {
        "CuDFFilterExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let f_exec = FilterExec::try_new(
            Arc::clone(self.host_exec.predicate()),
            children.swap_remove(0),
        )?;
        Ok(Arc::new(Self::try_new(f_exec)?))
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
    use crate::assert_snapshot;
    use crate::test_utils::TestFramework;

    #[tokio::test]
    #[ignore = "FilterExec execution fails because DataFusion cannot handle CuDF boolean arrays. \
                CuDF boolean arrays have empty ArrayData which fails DataFusion's validation. \
                The plan structure is correct (CuDFFilterExec is properly inserted), but execution \
                requires either: (1) a custom FilterExec that works with CuDF arrays directly, or \
                (2) converting boolean predicate results to host arrays before FilterExec uses them."]
    async fn test_basic_filter() -> Result<(), Box<dyn std::error::Error>> {
        let tf = TestFramework::new().await;

        let host_sql = r#"
            SELECT "MinTemp", "MaxTemp"
            FROM weather
            WHERE "MinTemp" > 10.0
            LIMIT 3
        "#;
        let cudf_sql = format!(
            r#"
            SET datafusion.execution.target_partitions=1;
            SET cudf.enable=true;
            {host_sql}
        "#
        );

        let plan = tf.plan(&cudf_sql).await?;
        assert_snapshot!(plan.display(), @r"
        CoalescePartitionsExec: fetch=3
          CoalesceBatchesExec: target_batch_size=8192, fetch=3
            CudfUnloadExec
              CuDFFilterExec: MinTemp@0 > 10
                CudfLoadExec
                  DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp], file_type=parquet, predicate=MinTemp@0 > 10, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 10, required_guarantees=[]
        ");

        let cudf_results = plan.execute().await?;
        assert_snapshot!(cudf_results.pretty_print, @"");

        let host_results = tf.execute(host_sql).await?;
        assert_eq!(host_results.pretty_print, cudf_results.pretty_print);

        Ok(())
    }
}
