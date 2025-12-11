use crate::errors::cudf_to_df;
use crate::expr::{columnar_value_to_cudf, expr_to_cudf_expr};
use arrow::array::{Array, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use datafusion::common::{exec_err, internal_err, Statistics};
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
use libcudf_rs::{apply_boolean_mask, CuDFColumnView};
use libcudf_rs::{CuDFColumnViewOrScalar, CuDFTableView};
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
    // Evaluate the predicate to get a boolean mask (CuDF array on GPU)
    let filter_array = predicate.evaluate(batch)?;
    let CuDFColumnViewOrScalar::ColumnView(bool_mask) = columnar_value_to_cudf(filter_array)?
    else {
        return internal_err!("Expected a CuDFColumnView from predicate evaluation for filter");
    };

    // The predicate must evaluate to a boolean array, otherwise something is wrong
    if bool_mask.data_type() != &DataType::Boolean {
        return exec_err!(
            "Expected CuDFColumnView predicate to evaluate to a boolean array, got: {}",
            bool_mask.data_type()
        );
    }

    // Check if the batch is already on GPU (all columns are CuDF arrays)
    let mut column_views: Vec<CuDFColumnView> = Vec::new();
    for (i, col) in batch.columns().iter().enumerate() {
        let Some(view) = col.as_any().downcast_ref::<CuDFColumnView>() else {
            return internal_err!(
                "Mixed GPU/host RecordBatch not supported: column {i} is not a CuDF array"
            );
        };
        column_views.push(view.clone());
    }

    let table_view = CuDFTableView::from_column_views(column_views).map_err(cudf_to_df)?;

    // Apply boolean mask using CuDF on GPU
    let filtered_table = apply_boolean_mask(&table_view, &bool_mask).map_err(cudf_to_df)?;

    // Keep data on GPU by wrapping table in an Arc and creating column views that reference it
    let table_view = filtered_table.into_view();
    let num_cols = table_view.num_columns();

    let mut cudf_columns: Vec<Arc<dyn Array>> = Vec::with_capacity(num_cols);
    for i in 0..num_cols {
        let col_view = table_view.column(i as i32);
        cudf_columns.push(Arc::new(col_view));
    }

    // Apply projection if needed
    if let Some(projection) = projection {
        let projected_columns: Vec<Arc<dyn Array>> = projection
            .iter()
            .map(|i| Arc::clone(&cudf_columns[*i]))
            .collect();
        Ok(RecordBatch::try_new(
            Arc::clone(output_schema),
            projected_columns,
        )?)
    } else {
        Ok(RecordBatch::try_new(batch.schema(), cudf_columns)?)
    }
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
    async fn test_basic_filter() -> Result<(), Box<dyn std::error::Error>> {
        let tf = TestFramework::new().await;

        let host_sql = r#"
            SET datafusion.execution.target_partitions = 1;
            SELECT "MinTemp", "MaxTemp"
            FROM weather
            WHERE "MinTemp" > 10.0
            LIMIT 3
        "#;
        let cudf_sql = format!(r#" SET cudf.enable=true; {host_sql} "#);

        let plan = tf.plan(&cudf_sql).await?;
        assert_snapshot!(plan.display(), @r"
        CoalescePartitionsExec: fetch=3
          CuDFUnloadExec
            CuDFCoalesceBatchesExec: target_batch_size=81920
              CuDFCoalesceBatchesExec: target_batch_size=8192, fetch=3
                CuDFFilterExec: MinTemp@0 > 10
                  CuDFLoadExec
                    CoalesceBatchesExec: target_batch_size=81920
                      DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp], file_type=parquet, predicate=MinTemp@0 > 10, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 10, required_guarantees=[]
        ");

        let cudf_results = plan.execute().await?;
        assert_snapshot!(cudf_results.pretty_print, @r"
        +---------+---------+
        | MinTemp | MaxTemp |
        +---------+---------+
        | 14.0    | 26.9    |
        | 13.7    | 23.4    |
        | 13.3    | 15.5    |
        +---------+---------+
        ");

        let host_results = tf.execute(host_sql).await?;
        assert_eq!(host_results.pretty_print, cudf_results.pretty_print);

        Ok(())
    }
}
