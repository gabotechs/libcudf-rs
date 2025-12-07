use crate::errors::cudf_to_df;
use arrow::array::{Array, RecordBatch};
use arrow_schema::SchemaRef;
use datafusion::common::{internal_err, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_physical_plan::coalesce::PushBatchStatus;
use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion_physical_plan::execution_plan::CardinalityEffect;
use datafusion_physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion_physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use delegate::delegate;
use futures_util::{ready, Stream, StreamExt};
use libcudf_rs::{is_cudf_array, is_cudf_record_batch, CuDFTable, CuDFTableView};
use std::any::Any;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct CuDFCoalesceBatchesExec {
    inner: CoalesceBatchesExec,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl CuDFCoalesceBatchesExec {
    pub fn new(inner: CoalesceBatchesExec) -> Self {
        Self {
            inner,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for CuDFCoalesceBatchesExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CuDF")?;
        self.inner.fmt_as(t, f)
    }
}

impl ExecutionPlan for CuDFCoalesceBatchesExec {
    fn name(&self) -> &str {
        "CuDFCoalesceBatchesExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let new =
            CoalesceBatchesExec::new(Arc::clone(&children[0]), self.inner.target_batch_size())
                .with_fetch(self.fetch());
        Ok(Arc::new(Self::new(new)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        Ok(Box::pin(CoalesceBatchesStream {
            input: self.inner.input().execute(partition, context)?,
            coalescer: LimitedBatchCoalescer::new(
                self.inner.input().schema(),
                self.inner.target_batch_size(),
                self.inner.fetch(),
            ),
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
            completed: false,
        }))
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        let new = self.inner.clone().with_fetch(limit);
        Some(Arc::new(Self::new(new)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    delegate! {
        to self.inner {
            fn properties(&self) -> &PlanProperties;
            fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>>;
            fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics, DataFusionError>;
            fn fetch(&self) -> Option<usize>;
            fn cardinality_effect(&self) -> CardinalityEffect;
            fn gather_filters_for_pushdown(&self, _phase: FilterPushdownPhase, parent_filters: Vec<Arc<dyn PhysicalExpr>>, _config: &ConfigOptions) -> Result<FilterDescription, DataFusionError>;
            fn handle_child_pushdown_result(&self, _phase: FilterPushdownPhase, child_pushdown_result: ChildPushdownResult, _config: &ConfigOptions) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>, DataFusionError>;
        }
    }
}

/// Stream for [`CoalesceBatchesExec`]. See [`CoalesceBatchesExec`] for more details.
struct CoalesceBatchesStream {
    /// The input plan
    input: SendableRecordBatchStream,
    /// Buffer for combining batches
    coalescer: LimitedBatchCoalescer,
    /// Execution metrics
    baseline_metrics: BaselineMetrics,
    /// is the input stream exhausted or limit reached?
    completed: bool,
}

impl Stream for CoalesceBatchesStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.baseline_metrics.record_poll(poll)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // we can't predict the size of incoming batches so re-use the size hint from the input
        self.input.size_hint()
    }
}

impl CoalesceBatchesStream {
    fn poll_next_inner(
        self: &mut Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch, DataFusionError>>> {
        let cloned_time = self.baseline_metrics.elapsed_compute().clone();
        loop {
            // If there is any completed batch ready, return it
            if let Some(batch) = self.coalescer.next_completed_batch() {
                return Poll::Ready(Some(Ok(batch)));
            }
            if self.completed {
                // If input is done and no batches are ready, return None to signal end of stream.
                return Poll::Ready(None);
            }
            // Attempt to pull the next batch from the input stream.
            let input_batch = ready!(self.input.poll_next_unpin(cx));
            // Start timing the operation. The timer records time upon being dropped.
            let _timer = cloned_time.timer();

            match input_batch {
                None => {
                    // Input stream is exhausted, finalize any remaining batches
                    self.completed = true;
                    self.coalescer.finish()?;
                }
                Some(Ok(batch)) => {
                    match self.coalescer.push_batch(batch)? {
                        PushBatchStatus::Continue => {
                            // Keep pushing more batches
                        }
                        PushBatchStatus::LimitReached => {
                            // limit was reached, so stop early
                            self.completed = true;
                            self.coalescer.finish()?;
                        }
                    }
                }
                // Error case
                other => return Poll::Ready(other),
            }
        }
    }
}

impl RecordBatchStream for CoalesceBatchesStream {
    fn schema(&self) -> SchemaRef {
        self.coalescer.schema()
    }
}

/// Concatenate multiple [`RecordBatch`]es and apply a limit
///
/// See [`BatchCoalescer`] for more details on how this works.
#[derive(Debug)]
pub struct LimitedBatchCoalescer {
    /// The arrow structure that builds the output batches
    inner: CuDFBatchCoalescer,
    /// Total number of rows returned so far
    total_rows: usize,
    /// Limit: maximum number of rows to fetch, `None` means fetch all rows
    fetch: Option<usize>,
    /// Indicates if the coalescer is finished
    finished: bool,
}

impl LimitedBatchCoalescer {
    /// Create a new `BatchCoalescer`
    ///
    /// # Arguments
    /// - `schema` - the schema of the output batches
    /// - `target_batch_size` - the minimum number of rows for each
    ///   output batch (until limit reached)
    /// - `fetch` - the maximum number of rows to fetch, `None` means fetch all rows
    pub fn new(schema: SchemaRef, target_batch_size: usize, fetch: Option<usize>) -> Self {
        Self {
            inner: CuDFBatchCoalescer::new(schema, target_batch_size)
                .with_biggest_coalesce_batch_size(Some(target_batch_size / 2)),
            total_rows: 0,
            fetch,
            finished: false,
        }
    }

    /// Return the schema of the output batches
    pub fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    /// Pushes the next [`RecordBatch`] into the coalescer and returns its status.
    ///
    /// # Arguments
    /// * `batch` - The [`RecordBatch`] to append.
    ///
    /// # Returns
    /// * [`PushBatchStatus::Continue`] - More batches can still be pushed.
    /// * [`PushBatchStatus::LimitReached`] - The row limit was reached after processing
    ///   this batch. The caller should call [`Self::finish`] before retrieving the
    ///   remaining buffered batches.
    ///
    /// # Errors
    /// Returns an error if called after [`Self::finish`] or if the internal push
    /// operation fails.
    pub fn push_batch(&mut self, batch: RecordBatch) -> Result<PushBatchStatus, DataFusionError> {
        if self.finished {
            return internal_err!("LimitedBatchCoalescer: cannot push batch after finish");
        }

        // if we are at the limit, return LimitReached
        if let Some(fetch) = self.fetch {
            // limit previously reached
            if self.total_rows >= fetch {
                return Ok(PushBatchStatus::LimitReached);
            }

            // limit now reached
            if self.total_rows + batch.num_rows() >= fetch {
                // Limit is reached
                let remaining_rows = fetch - self.total_rows;
                debug_assert!(remaining_rows > 0);

                let batch_head = batch.slice(0, remaining_rows);
                self.total_rows += batch_head.num_rows();
                self.inner.push_batch(batch_head)?;
                return Ok(PushBatchStatus::LimitReached);
            }
        }

        // Limit not reached, push the entire batch
        self.total_rows += batch.num_rows();
        self.inner.push_batch(batch)?;

        Ok(PushBatchStatus::Continue)
    }

    /// Return true if there is no data buffered
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Complete the current buffered batch and finish the coalescer
    ///
    /// Any subsequent calls to `push_batch()` will return an Err
    pub fn finish(&mut self) -> Result<(), DataFusionError> {
        self.inner.finish_buffered_batch()?;
        self.finished = true;
        Ok(())
    }

    /// Return the next completed batch, if any
    pub fn next_completed_batch(&mut self) -> Option<RecordBatch> {
        self.inner.next_completed_batch()
    }
}

#[derive(Debug)]
pub struct CuDFBatchCoalescer {
    schema: SchemaRef,
    target_batch_size: usize,
    buffered_batches: Vec<RecordBatch>,
    buffered_rows: usize,
    completed: Vec<RecordBatch>,
    biggest_coalesce_batch_size: Option<usize>,
}

impl CuDFBatchCoalescer {
    pub fn new(schema: SchemaRef, target_batch_size: usize) -> Self {
        Self {
            schema,
            target_batch_size,
            buffered_batches: Vec::new(),
            buffered_rows: 0,
            completed: Vec::new(),
            biggest_coalesce_batch_size: None,
        }
    }

    pub fn with_biggest_coalesce_batch_size(mut self, limit: Option<usize>) -> Self {
        self.biggest_coalesce_batch_size = limit;
        self
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    pub fn push_batch(&mut self, batch: RecordBatch) -> Result<(), DataFusionError> {
        let batch_size = batch.num_rows();

        if batch_size == 0 {
            return Ok(());
        }

        // Large batch optimization
        if let Some(limit) = self.biggest_coalesce_batch_size {
            if batch_size > limit {
                if self.buffered_rows == 0 {
                    self.completed.push(batch);
                    return Ok(());
                }

                if self.buffered_rows > limit {
                    self.finish_buffered_batch()?;
                    self.completed.push(batch);
                    return Ok(());
                }
            }
        }

        // Add batch to buffer
        self.buffered_rows += batch_size;
        self.buffered_batches.push(batch);

        // If we have enough rows, flush
        while self.buffered_rows >= self.target_batch_size {
            self.flush_target_batch()?;
        }

        Ok(())
    }

    fn flush_target_batch(&mut self) -> Result<(), DataFusionError> {
        if self.buffered_batches.is_empty() {
            return Ok(());
        }

        let total_rows: usize = self.buffered_batches.iter().map(|b| b.num_rows()).sum();

        if total_rows < self.target_batch_size {
            return Ok(());
        }

        // Convert to CuDF table views
        let table_views: Result<Vec<_>, _> = self
            .buffered_batches
            .iter()
            .map(|batch| {
                if !is_cudf_record_batch(batch) {
                    return internal_err!("Expected CuDF arrays in batch");
                }
                CuDFTableView::from_record_batch(batch).map_err(cudf_to_df)
            })
            .collect();
        let table_views = table_views?;

        // Concatenate all buffered batches
        let concat_table = CuDFTable::concat(table_views).map_err(cudf_to_df)?;

        // Convert back to record batch
        let concat_view = concat_table.into_view();
        let concat_batch = concat_view.to_record_batch().map_err(cudf_to_df)?;

        // Split at target_batch_size
        let output_batch = concat_batch.slice(0, self.target_batch_size);
        let remaining_rows = concat_batch.num_rows() - self.target_batch_size;

        self.completed.push(output_batch);

        // Update buffer with remaining rows
        if remaining_rows > 0 {
            let remaining_batch = concat_batch.slice(self.target_batch_size, remaining_rows);
            self.buffered_batches = vec![remaining_batch];
            self.buffered_rows = remaining_rows;
        } else {
            self.buffered_batches.clear();
            self.buffered_rows = 0;
        }

        Ok(())
    }

    pub fn finish_buffered_batch(&mut self) -> Result<(), DataFusionError> {
        if self.buffered_batches.is_empty() {
            return Ok(());
        }

        if self.buffered_batches.len() == 1 {
            self.completed.push(self.buffered_batches.pop().unwrap());
            self.buffered_rows = 0;
            return Ok(());
        }

        // Convert to CuDF table views and concatenate
        let table_views: Result<Vec<_>, _> = self
            .buffered_batches
            .iter()
            .map(|batch| {
                if !is_cudf_array(batch.column(0)) {
                    return internal_err!("Expected CuDF arrays in batch");
                }
                CuDFTableView::from_record_batch(batch).map_err(cudf_to_df)
            })
            .collect();
        let table_views = table_views?;

        let concat_table = CuDFTable::concat(table_views).map_err(cudf_to_df)?;
        let concat_view = concat_table.into_view();
        let concat_batch = concat_view.to_record_batch().map_err(cudf_to_df)?;

        self.completed.push(concat_batch);
        self.buffered_batches.clear();
        self.buffered_rows = 0;

        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.buffered_rows == 0 && self.completed.is_empty()
    }

    pub fn next_completed_batch(&mut self) -> Option<RecordBatch> {
        if self.completed.is_empty() {
            None
        } else {
            Some(self.completed.remove(0))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::optimizer::CuDFBoundariesRule;
    use crate::physical::CuDFCoalesceBatchesExec;
    use crate::test_utils::TestFramework;
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
    use datafusion_physical_plan::{execute_stream, ExecutionPlan};
    use futures_util::TryStreamExt;
    use std::error::Error;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_coalesce_batches() -> Result<(), Box<dyn Error>> {
        let tf = TestFramework::new().await;

        let plan = tf.plan("SELECT * FROM weather LIMIT 20").await?;
        let host_plan = host_coalesce(Arc::clone(&plan.plan), 10, None);
        let cudf_plan = cudf_coalesce(Arc::clone(&plan.plan), 10, None);
        exact_same_batches(&tf, host_plan, cudf_plan).await
    }

    fn host_coalesce(
        plan: Arc<dyn ExecutionPlan>,
        size: usize,
        fetch: Option<usize>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(CoalesceBatchesExec::new(plan, size).with_fetch(fetch))
    }

    fn cudf_coalesce(
        plan: Arc<dyn ExecutionPlan>,
        size: usize,
        fetch: Option<usize>,
    ) -> Arc<dyn ExecutionPlan> {
        let inner = CoalesceBatchesExec::new(plan, size).with_fetch(fetch);
        Arc::new(CuDFCoalesceBatchesExec::new(inner))
    }

    async fn exact_same_batches(
        tf: &TestFramework,
        host: Arc<dyn ExecutionPlan>,
        cudf: Arc<dyn ExecutionPlan>,
    ) -> Result<(), Box<dyn Error>> {
        let host_stream = execute_stream(host, tf.task_ctx())?;
        let host_batches = host_stream.try_collect::<Vec<_>>().await?;

        let cudf = CuDFBoundariesRule.optimize(cudf, &Default::default())?;
        let cudf_stream = execute_stream(cudf, tf.task_ctx())?;
        let cudf_batches = cudf_stream.try_collect::<Vec<_>>().await?;

        for (host, cudf) in host_batches.into_iter().zip(cudf_batches.into_iter()) {
            assert_eq!(
                pretty_format_batches(&[host])?.to_string(),
                pretty_format_batches(&[cudf])?.to_string()
            );
        }

        Ok(())
    }
}
