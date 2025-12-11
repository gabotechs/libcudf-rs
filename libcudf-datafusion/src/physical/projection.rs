use crate::expr::expr_to_cudf_expr;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatchOptions;
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::execution::context::DataFilePaths;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::equivalence::ProjectionMapping;
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion_physical_plan::execution_plan::CardinalityEffect;
use datafusion_physical_plan::filter_pushdown::{FilterDescription, FilterPushdownPhase};
use datafusion_physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion_physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PhysicalExpr,
    PlanProperties,
};
use delegate::delegate;
use futures::stream::{Stream, StreamExt};
use std::any::Any;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct CuDFProjectionExec {
    host_exec: ProjectionExec,
    cudf_exprs: ProjectionExprs,
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

impl CuDFProjectionExec {
    pub fn try_new(host_exec: ProjectionExec) -> Result<Self, DataFusionError> {
        let cudf_exprs = host_exec
            .expr()
            .iter()
            .map(|v| {
                Ok::<_, DataFusionError>(ProjectionExpr {
                    alias: v.alias.clone(),
                    expr: expr_to_cudf_expr(v.expr.as_ref())?,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let projection_exprs = ProjectionExprs::new(cudf_exprs);
        let input_schema = host_exec.input().schema();
        let output_schema = Arc::new(projection_exprs.project_schema(&input_schema)?);

        // Construct a map from the input expressions to the output expression of the Projection
        let projection_mapping = projection_exprs.projection_mapping(&input_schema)?;
        let properties =
            Self::compute_properties(host_exec.input(), &projection_mapping, output_schema)?;

        Ok(Self {
            host_exec,
            cudf_exprs: projection_exprs,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        projection_mapping: &ProjectionMapping,
        schema: SchemaRef,
    ) -> Result<PlanProperties, DataFusionError> {
        // Calculate equivalence properties:
        let input_eq_properties = input.equivalence_properties();
        let eq_properties = input_eq_properties.project(projection_mapping, schema);
        // Calculate output partitioning, which needs to respect aliases:
        let output_partitioning = input
            .output_partitioning()
            .project(projection_mapping, input_eq_properties);

        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            input.pipeline_behavior(),
            input.boundedness(),
        ))
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
        let input = self.host_exec.input().execute(partition, context)?;
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        Ok(Box::pin(CuDFProjectionStream {
            schema: self.schema(),
            expr: self.cudf_exprs.expr_iter().collect(),
            input,
            baseline_metrics,
        }))
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    delegate! {
        to self.host_exec {
            fn maintains_input_order(&self) -> Vec<bool>;
            fn benefits_from_input_partitioning(&self) -> Vec<bool>;
            fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>>;
            fn cardinality_effect(&self) -> CardinalityEffect;
            fn supports_limit_pushdown(&self) -> bool;
            fn gather_filters_for_pushdown(&self, phase: FilterPushdownPhase, parent_filters: Vec<Arc<dyn PhysicalExpr>>, config: &ConfigOptions) -> Result<FilterDescription, DataFusionError>;
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

struct CuDFProjectionStream {
    schema: SchemaRef,
    expr: Vec<Arc<dyn PhysicalExpr>>,
    input: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
}

impl CuDFProjectionStream {
    fn batch_project(&self, batch: &RecordBatch) -> Result<RecordBatch, DataFusionError> {
        // Records time on drop
        let _timer = self.baseline_metrics.elapsed_compute().timer();
        let arrays = self
            .expr
            .iter()
            .map(|expr| {
                expr.evaluate(batch)
                    .and_then(|v| v.into_array(batch.num_rows()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        if arrays.is_empty() {
            let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
            RecordBatch::try_new_with_options(Arc::clone(&self.schema), arrays, &options)
        } else {
            RecordBatch::try_new(Arc::clone(&self.schema), arrays)
        }
        .map_err(|err| {
            DataFusionError::ArrowError(
                Box::new(err),
                Some("Error projecting CuDF RecordBatch".to_string()),
            )
        })
    }
}

impl Stream for CuDFProjectionStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.input.poll_next_unpin(cx).map(|x| match x {
            Some(Ok(batch)) => Some(self.batch_project(&batch)),
            other => other,
        });

        self.baseline_metrics.record_poll(poll)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.input.size_hint()
    }
}

impl RecordBatchStream for CuDFProjectionStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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
            .plan(r#" SET cudf.enable=true; SELECT "MinTemp" + 1 FROM weather LIMIT 1"#)
            .await?;

        assert_snapshot!(plan.display(), @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[MinTemp@0 + 1 as weather.MinTemp + Int64(1)]
            CuDFLoadExec
              CoalesceBatchesExec: target_batch_size=81920
                RepartitionExec: partitioning=RoundRobinBatch(16), input_partitions=1
                  DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000002.parquet]]}, projection=[MinTemp], limit=1, file_type=parquet
        ");
        let result = plan.execute().await?;
        assert_snapshot!(result.pretty_print, @r"
        +----------------------------+
        | weather.MinTemp + Int64(1) |
        +----------------------------+
        | 7.6                        |
        +----------------------------+
        ");
        let host_result = tf
            .execute(r#"SELECT "MinTemp" + 1 FROM weather LIMIT 1"#)
            .await?;
        assert_eq!(host_result.pretty_print, result.pretty_print);
        Ok(())
    }

    #[tokio::test]
    async fn test_decimal_addition() -> Result<(), DataFusionError> {
        let tf = TestFramework::new().await;

        tf.execute(
            r#"CREATE TABLE prices (price1 DECIMAL(10, 2), price2 DECIMAL(10, 2)) AS VALUES
                (123.45, 100.00),
                (678.90, 200.50),
                (111.11, 50.25)"#,
        )
        .await?;

        let result = tf
            .execute(r#"SET cudf.enable=true; SELECT price1 + price2 as total FROM prices"#)
            .await?;

        assert_snapshot!(result.pretty_print, @r"
        +--------+
        | total  |
        +--------+
        | 223.45 |
        | 879.40 |
        | 161.36 |
        +--------+
        ");

        let host_result = tf
            .execute(r#"SELECT price1 + price2 as total FROM prices"#)
            .await?;
        assert_eq!(host_result.pretty_print, result.pretty_print);

        Ok(())
    }
}
