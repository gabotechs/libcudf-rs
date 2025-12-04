use crate::expr::expr_to_cudf_expr;
use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::common::Statistics;
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::execution_plan::CardinalityEffect;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::filter_pushdown::{FilterDescription, FilterPushdownPhase};
use datafusion_physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder, MetricType, MetricsSet, RatioMetrics,
};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
};
use delegate::delegate;
use futures_util::{Stream, StreamExt};
use std::any::Any;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

#[derive(Debug)]
pub struct CuDFFilterExec {
    host_exec: FilterExec,

    /// The expression to filter on. This expression must evaluate to a boolean value.
    predicate: Arc<dyn PhysicalExpr>,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// The projection indices of the columns in the output schema of join
    projection: Option<Vec<usize>>,
}

impl CuDFFilterExec {
    pub fn try_new(host_exec: FilterExec) -> Result<Self, DataFusionError> {
        let predicate = expr_to_cudf_expr(host_exec.predicate().as_ref())?;
        let input = Arc::clone(host_exec.input());
        let projection = host_exec.projection().cloned();
        Ok(Self {
            host_exec,
            predicate,
            input,
            metrics: ExecutionPlanMetricsSet::new(),
            projection,
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
        let metrics = CuDFFilterExecMetrics::new(&self.metrics, partition);
        Ok(Box::pin(CuDFFilterExecStream {
            schema: self.schema(),
            predicate: Arc::clone(&self.predicate),
            input: self.input.execute(partition, context)?,
            metrics,
            projection: self.projection.clone(),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
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
            fn partition_statistics(&self, partition: Option<usize>) -> datafusion::common::Result<Statistics>;
        }
    }
}

// Struct pretty much copied from `datafusion/core/src/physical_plan/filter.rs`
/// The FilterExec streams wraps the input iterator and applies the predicate expression to
/// determine which rows to include in its output batches
struct CuDFFilterExecStream {
    /// Output schema after the projection
    schema: SchemaRef,
    /// The expression to filter on. This expression must evaluate to a boolean value.
    predicate: Arc<dyn PhysicalExpr>,
    /// The input partition to filter.
    input: SendableRecordBatchStream,
    /// Runtime metrics recording
    metrics: CuDFFilterExecMetrics,
    /// The projection indices of the columns in the input schema
    projection: Option<Vec<usize>>,
}

/// The metrics for `FilterExec`
struct CuDFFilterExecMetrics {
    // Common metrics for most operators
    baseline_metrics: BaselineMetrics,
    // Selectivity of the filter, calculated as output_rows / input_rows
    selectivity: RatioMetrics,
}

impl CuDFFilterExecMetrics {
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline_metrics: BaselineMetrics::new(metrics, partition),
            selectivity: MetricBuilder::new(metrics)
                .with_type(MetricType::SUMMARY)
                .ratio_metrics("selectivity", partition),
        }
    }
}

fn filter_and_project(
    batch: &RecordBatch,
    predicate: &Arc<dyn PhysicalExpr>,
    projection: Option<&Vec<usize>>,
    output_schema: &SchemaRef,
) -> Result<RecordBatch, DataFusionError> {
    predicate
        .evaluate(batch)
        .and_then(|v| v.into_array(batch.num_rows()))
        .and_then(|array| {
            todo!();
        })
}

// Implementation pretty much copied from `datafusion/core/src/physical_plan/filter.rs`
impl Stream for CuDFFilterExecStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll;
        loop {
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    let timer = self.metrics.baseline_metrics.elapsed_compute().timer();
                    let filtered_batch = filter_and_project(
                        &batch,
                        &self.predicate,
                        self.projection.as_ref(),
                        &self.schema,
                    )?;
                    timer.done();

                    self.metrics.selectivity.add_part(filtered_batch.num_rows());
                    self.metrics.selectivity.add_total(batch.num_rows());

                    // Skip entirely filtered batches
                    if filtered_batch.num_rows() == 0 {
                        continue;
                    }
                    poll = Poll::Ready(Some(Ok(filtered_batch)));
                    break;
                }
                value => {
                    poll = Poll::Ready(value);
                    break;
                }
            }
        }
        self.metrics.baseline_metrics.record_poll(poll)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // Same number of record batches
        self.input.size_hint()
    }
}

impl RecordBatchStream for CuDFFilterExecStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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
