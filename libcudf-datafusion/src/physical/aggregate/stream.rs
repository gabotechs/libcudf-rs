use crate::errors::cudf_to_df;
use crate::metrics::CuDFBaselineMetrics;
use crate::physical::aggregate::{
    PreparedAggregateOutputKind, PreparedCuDFAggregate, StateColumnRef,
};
use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use datafusion::common::{exec_err, internal_err};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion::physical_expr_common::metrics::{
    ExecutionPlanMetricsSet, MetricBuilder, MetricType, RatioMetrics, Time,
};
use datafusion_physical_plan::aggregates::{evaluate_group_by, evaluate_many, AggregateMode};
use datafusion_physical_plan::PhysicalExpr;
use futures::StreamExt;
use libcudf_rs::{
    global_execution_stream, record_batch_execution_stream, record_batch_with_schema, CuDFColumn,
    CuDFColumnView, CuDFGroupBy, CuDFStream, CuDFTable, CuDFTableView,
};
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::task::{JoinError, JoinHandle};

/// Aggregate-specific timers. Mirrors upstream `GroupByMetrics` from
/// `datafusion::physical_plan::aggregates::group_values::metrics`: same
/// field names and same `subset_time` metric keys, so EXPLAIN ANALYZE
/// looks identical to a CPU `AggregateExec`.
#[derive(Clone)]
pub(crate) struct GroupByMetrics {
    /// Time spent calculating the group IDs from the evaluated grouping columns.
    pub(crate) time_calculating_group_ids: Time,
    /// Time spent evaluating the inputs to the aggregate functions.
    pub(crate) aggregate_arguments_time: Time,
    /// Time spent evaluating the aggregate expressions themselves
    /// (e.g. summing all elements and counting number of elements for `avg` aggregate).
    pub(crate) aggregation_time: Time,
    /// Time spent emitting the final results and constructing the record batch
    /// which includes finalizing the grouping expressions
    /// (e.g. emit from the hash table in case of hash aggregation) and the accumulators.
    pub(crate) emitting_time: Time,
}

impl GroupByMetrics {
    pub(crate) fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            time_calculating_group_ids: MetricBuilder::new(metrics)
                .subset_time("time_calculating_group_ids", partition),
            aggregate_arguments_time: MetricBuilder::new(metrics)
                .subset_time("aggregate_arguments_time", partition),
            aggregation_time: MetricBuilder::new(metrics)
                .subset_time("aggregation_time", partition),
            emitting_time: MetricBuilder::new(metrics).subset_time("emitting_time", partition),
        }
    }
}

/// Maps each physical aggregation op to its slice of the flat state_columns array.
///
/// Built once at construction from `num_state_columns()`.
///
/// Example for physical aggregates `[SUM, AVG, MAX]`:
/// ```text
///   SUM -> (0, 1), AVG -> (1, 2), MAX -> (3, 1)
/// ```
#[derive(Clone)]
struct ColumnMapping {
    ranges: Vec<(usize, usize)>, // (start, count) per op
}

/// Running O(G) state: unique group keys + intermediate state columns.
struct RunningState {
    keys: CuDFTable,
    state_columns: Vec<CuDFColumn>, // flat, indexed via ColumnMapping
}

/// GPU-accelerated GROUP BY aggregation stream using chunked aggregation.
///
/// Input batches are assigned to a lane by their attached CUDA stream. Each
/// lane buffers and aggregates independently, allowing work on multiple streams to
/// overlap while keeping a separate O(G) partial state per stream. At end of
/// input, the lane states are synchronized and merged on one stream before the
/// single output batch is emitted.
pub struct CuDFAggregateStream {
    input: SendableRecordBatchStream,
    output_schema: SchemaRef,
    state: StreamState,
    worker_template: AggregateWorker,
    lanes: Vec<AggregateLane>,
    input_exhausted: bool,
    /// Target input bytes accumulated before running an aggregate/merge cycle.
    chunk_target_bytes: usize,
    /// Output rows/bytes/batches + total elapsed_compute, GPU-safe.
    baseline_metrics: CuDFBaselineMetrics,
    /// Partial-mode-only ratio of input rows to output rows. `None` for
    /// `Single`/`Final*` modes where the metric is not meaningful.
    reduction_factor: Option<RatioMetrics>,
}

/// Buffered input and partial aggregate state for one CUDA stream.
struct AggregateLane {
    stream: CuDFStream,
    worker: Option<AggregateWorker>,
    task: Option<JoinHandle<Result<AggregateWorker>>>,
    pending_batches: Vec<RecordBatch>,
    pending_bytes: usize,
}

/// State moved to a blocking pool while doing cuDF work / waiting on kernels.
struct AggregateWorker {
    output_schema: SchemaRef,
    prepared: PreparedCuDFAggregate,
    aggregate_args: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    column_mapping: ColumnMapping,
    running: Option<RunningState>,
    group_by_metrics: GroupByMetrics,
}

impl CuDFAggregateStream {
    pub fn new(
        input: SendableRecordBatchStream,
        output_schema: SchemaRef,
        prepared: PreparedCuDFAggregate,
        chunk_target_bytes: usize,
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Result<Self> {
        let aggregate_args = prepared
            .aggs
            .iter()
            .map(|agg| agg.args.clone())
            .collect::<Vec<_>>();

        let column_mapping = {
            let mut offset = 0;
            let ranges = prepared
                .aggs
                .iter()
                .map(|agg| {
                    let count = agg.op.num_state_columns();
                    let start = offset;
                    offset += count;
                    (start, count)
                })
                .collect();
            ColumnMapping { ranges }
        };

        let baseline_metrics = CuDFBaselineMetrics::new(metrics, partition);
        let group_by_metrics = GroupByMetrics::new(metrics, partition);
        let reduction_factor = (prepared.mode == AggregateMode::Partial).then(|| {
            MetricBuilder::new(metrics)
                .with_type(MetricType::Summary)
                .ratio_metrics("reduction_factor", partition)
        });

        let worker_template = AggregateWorker {
            output_schema: Arc::clone(&output_schema),
            prepared: prepared.clone(),
            aggregate_args: aggregate_args.clone(),
            column_mapping: column_mapping.clone(),
            running: None,
            group_by_metrics: group_by_metrics.clone(),
        };

        Ok(Self {
            input,
            output_schema: Arc::clone(&output_schema),
            state: StreamState::Running,
            worker_template,
            lanes: Vec::new(),
            input_exhausted: false,
            chunk_target_bytes: chunk_target_bytes.max(1),
            baseline_metrics,
            reduction_factor,
        })
    }

    fn lane_for_batch(&mut self, batch: &RecordBatch) -> Result<usize> {
        let stream = record_batch_execution_stream(batch)
            .map_err(cudf_to_df)?
            .unwrap_or(global_execution_stream().map_err(cudf_to_df)?);

        if let Some(index) = self
            .lanes
            .iter()
            .position(|lane| lane.stream.ptr_eq(&stream))
        {
            return Ok(index);
        }

        self.lanes.push(AggregateLane {
            stream,
            worker: Some(self.worker_template.new_lane()),
            task: None,
            pending_batches: Vec::new(),
            pending_bytes: 0,
        });
        Ok(self.lanes.len() - 1)
    }

    fn start_aggregating_lane(&mut self, lane_index: usize) {
        let lane = &mut self.lanes[lane_index];
        let pending_batches = mem::take(&mut lane.pending_batches);
        lane.pending_bytes = 0;
        let mut worker = lane
            .worker
            .take()
            .expect("aggregate worker must be available before aggregating a chunk");
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();

        let task = tokio::task::spawn_blocking(move || {
            let _timer = elapsed_compute.timer();
            worker.aggregate_chunk(pending_batches)?;
            Ok(worker)
        });
        lane.task = Some(task);
    }

    fn start_ready_lane_tasks(&mut self, flush: bool) -> bool {
        let ready = self
            .lanes
            .iter()
            .enumerate()
            .filter_map(|(index, lane)| {
                (lane.task.is_none()
                    && lane.worker.is_some()
                    && !lane.pending_batches.is_empty()
                    && (flush || lane.pending_bytes >= self.chunk_target_bytes))
                    .then_some(index)
            })
            .collect::<Vec<_>>();
        for index in &ready {
            self.start_aggregating_lane(*index);
        }
        !ready.is_empty()
    }

    fn poll_lane_tasks(&mut self, cx: &mut Context<'_>) -> Result<()> {
        for lane in &mut self.lanes {
            let Some(task) = lane.task.as_mut() else {
                continue;
            };
            match Pin::new(task).poll(cx) {
                Poll::Pending => {}
                Poll::Ready(Ok(Ok(worker))) => {
                    lane.task = None;
                    lane.worker = Some(worker);
                }
                Poll::Ready(Ok(Err(error))) => return Err(error),
                Poll::Ready(Err(error)) => return Err(blocking_task_error(error)),
            }
        }
        Ok(())
    }

    fn is_backpressured(&self) -> bool {
        self.lanes.iter().any(|lane| {
            lane.task.is_some()
                && !lane.pending_batches.is_empty()
                && lane.pending_bytes >= self.chunk_target_bytes
        })
    }

    fn start_finalizing(&mut self) {
        let workers = self
            .lanes
            .drain(..)
            .map(|mut lane| {
                (
                    lane.stream,
                    lane.worker
                        .take()
                        .expect("aggregate worker must be available before finalization"),
                )
            })
            .collect();
        let empty_worker = self.worker_template.new_lane();
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();

        let task = tokio::task::spawn_blocking(move || {
            let _timer = elapsed_compute.timer();
            finalize_workers(workers, empty_worker)
        });
        self.state = StreamState::Finalizing(task);
    }
}

impl AggregateWorker {
    fn new_lane(&self) -> Self {
        Self {
            output_schema: Arc::clone(&self.output_schema),
            prepared: self.prepared.clone(),
            aggregate_args: self.aggregate_args.clone(),
            column_mapping: self.column_mapping.clone(),
            running: None,
            group_by_metrics: self.group_by_metrics.clone(),
        }
    }

    /// Concatenate the buffered batches, aggregate the resulting chunk, and merge
    /// its partial state into the running aggregate.
    fn aggregate_chunk(&mut self, pending_batches: Vec<RecordBatch>) -> Result<()> {
        if pending_batches.is_empty() {
            return Ok(());
        }

        let chunk = concat_cudf_batches(&pending_batches)?;

        let group_by = self.evaluate_batch_groups(&chunk)?;
        let evaluated_args = self.evaluate_batch_arguments(&chunk)?;
        let requests = self.build_batch_requests(evaluated_args)?;

        let (chunk_keys, chunk_results) = {
            let _timer = self.group_by_metrics.aggregation_time.timer();
            group_by.aggregate(requests).map_err(cudf_to_df)?
        };
        let mut chunk_state_columns = chunk_results.into_iter().flatten().collect();

        // Normalize partial state column types so they are compatible with merge_requests.
        if !matches!(
            self.prepared.mode,
            AggregateMode::Final | AggregateMode::FinalPartitioned
        ) {
            chunk_state_columns = self.normalize_partial_state(chunk_state_columns)?;
        }

        self.merge_into_running(chunk_keys, chunk_state_columns)
    }

    /// Dispatch `normalize_partial_state` for each op over the flat state column vec.
    fn normalize_partial_state(&self, cols: Vec<CuDFColumn>) -> Result<Vec<CuDFColumn>> {
        let mut result = Vec::with_capacity(cols.len());
        let mut col_iter = cols.into_iter();
        for op_idx in 0..self.prepared.aggs.len() {
            let (_, count) = self.column_mapping.ranges[op_idx];
            let op_cols: Vec<CuDFColumn> = col_iter.by_ref().take(count).collect();
            result.extend(
                self.prepared.aggs[op_idx]
                    .op
                    .normalize_partial_state(op_cols)?,
            );
        }
        Ok(result)
    }

    /// Build aggregation requests for a chunk based on mode.
    ///
    /// - Single/Partial/SinglePartitioned: use `partial_requests` (input is raw data)
    /// - Final/FinalPartitioned: use `merge_requests` (input is partial state)
    fn build_batch_requests(
        &self,
        evaluated_args: Vec<Vec<CuDFColumnView>>,
    ) -> Result<Vec<libcudf_rs::AggregationRequest>> {
        let mut requests = Vec::new();

        let use_merge = matches!(
            self.prepared.mode,
            AggregateMode::Final | AggregateMode::FinalPartitioned
        );

        for (agg, args) in self.prepared.aggs.iter().zip(evaluated_args) {
            let op_requests = if use_merge {
                agg.op.merge_requests(&args)?
            } else {
                agg.op.partial_requests(&args)?
            };
            requests.extend(op_requests);
        }

        Ok(requests)
    }

    /// Merge new chunk results into the running state.
    ///
    /// If no running state exists, stores the new results directly.
    /// Otherwise, concatenates running + new, then re-aggregates with merge_requests.
    fn merge_into_running(
        &mut self,
        new_keys: CuDFTable,
        new_state_columns: Vec<CuDFColumn>,
    ) -> Result<()> {
        let Some(running) = self.running.take() else {
            self.running = Some(RunningState {
                keys: new_keys,
                state_columns: new_state_columns,
            });
            return Ok(());
        };

        // Concat keys
        let combined_keys = CuDFTable::concat(vec![running.keys.into_view(), new_keys.into_view()])
            .map_err(cudf_to_df)?;

        // Concat each state column pair
        let mut combined_state_columns = Vec::with_capacity(running.state_columns.len());
        for (run_col, new_col) in running.state_columns.into_iter().zip(new_state_columns) {
            let combined = CuDFColumn::concat(vec![run_col.into_view(), new_col.into_view()])
                .map_err(cudf_to_df)?;
            combined_state_columns.push(combined);
        }

        // Convert to views for merge requests
        let combined_views: Vec<CuDFColumnView> = combined_state_columns
            .into_iter()
            .map(|col| col.into_view())
            .collect();

        // Build merge requests
        let mut requests = Vec::new();
        for (op_idx, agg) in self.prepared.aggs.iter().enumerate() {
            let (start, count) = self.column_mapping.ranges[op_idx];
            let state_views: Vec<CuDFColumnView> = combined_views[start..start + count].to_vec();
            requests.extend(agg.op.merge_requests(&state_views)?);
        }

        // Re-aggregate
        let group_by = CuDFGroupBy::from_table_view(combined_keys.into_view());
        let (merged_keys, merged_results) = {
            let _timer = self.group_by_metrics.aggregation_time.timer();
            group_by.aggregate(requests).map_err(cudf_to_df)?
        };
        let merged_state_columns = merged_results.into_iter().flatten().collect();

        self.running = Some(RunningState {
            keys: merged_keys,
            state_columns: merged_state_columns,
        });

        Ok(())
    }

    /// Build the final output RecordBatch from the running state.
    fn build_output(&mut self) -> Result<Option<RecordBatch>> {
        let Some(running) = self.running.take() else {
            return Ok(None);
        };

        let _timer = self.group_by_metrics.emitting_time.timer();
        let num_rows = running.keys.num_rows();
        let key_columns = running.keys.into_columns().map_err(cudf_to_df)?;
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.output_schema.fields().len());

        for col in key_columns {
            arrays.push(Arc::new(col.into_view()));
        }

        let state_views: Vec<CuDFColumnView> = running
            .state_columns
            .into_iter()
            .map(|c| c.into_view())
            .collect();

        let is_partial = matches!(self.prepared.mode, AggregateMode::Partial);

        for output in &self.prepared.outputs {
            match &output.kind {
                PreparedAggregateOutputKind::Direct { physical } => {
                    let agg = &self.prepared.aggs[*physical];
                    let state_views = self.state_slice(&state_views, *physical);

                    if is_partial {
                        let state_fields = output.expr.state_fields()?;
                        for (col_idx, view) in state_views.into_iter().enumerate() {
                            let target_type = state_fields[col_idx].data_type();
                            if view.data_type() != target_type {
                                let casted =
                                    libcudf_rs::cast(&view, target_type).map_err(cudf_to_df)?;
                                arrays.push(Arc::new(casted.into_view()));
                            } else {
                                arrays.push(Arc::new(view));
                            }
                        }
                    } else {
                        let finalized = agg.op.finalize(&state_views, &agg.output_type)?;
                        arrays.push(Arc::new(finalized));
                    }
                }
                PreparedAggregateOutputKind::Derived {
                    op,
                    state,
                    output_type,
                } => {
                    if is_partial {
                        return internal_err!(
                            "Derived aggregate output is not valid for Partial mode"
                        );
                    }
                    let state_views = state
                        .iter()
                        .map(|state| self.state_column(&state_views, *state))
                        .collect::<Vec<_>>();
                    let finalized = op.finalize(&state_views, output_type)?;
                    arrays.push(Arc::new(finalized));
                }
            }
        }

        Ok(Some(record_batch_with_schema(
            arrays,
            &self.output_schema,
            num_rows,
        )?))
    }

    fn state_slice(&self, state_views: &[CuDFColumnView], aggregate: usize) -> Vec<CuDFColumnView> {
        let (start, count) = self.column_mapping.ranges[aggregate];
        state_views[start..start + count].to_vec()
    }

    fn state_column(
        &self,
        state_views: &[CuDFColumnView],
        state: StateColumnRef,
    ) -> CuDFColumnView {
        let (start, _) = self.column_mapping.ranges[state.aggregate];
        state_views[start + state.column].clone()
    }

    /// Evaluate GROUP BY expressions on a batch and wrap the resulting key
    /// columns into a [`CuDFGroupBy`] for GPU aggregation.
    fn evaluate_batch_groups(&self, batch: &RecordBatch) -> Result<CuDFGroupBy> {
        let _timer = self.group_by_metrics.time_calculating_group_ids.timer();
        let grouping_sets = evaluate_group_by(&self.prepared.group_by, batch)?;

        if grouping_sets.len() != 1 {
            return exec_err!("Expected single grouping set, got {}", grouping_sets.len());
        }

        let group = &grouping_sets[0];
        let column_views = group
            .iter()
            .map(|arr| {
                let Some(view) = arr.as_any().downcast_ref::<CuDFColumnView>() else {
                    return internal_err!("Expected Array to be of type CuDFColumnView");
                };
                Ok(view.clone())
            })
            .collect::<Result<Vec<_>>>()?;

        let table_view = CuDFTableView::try_from_column_views(column_views).map_err(cudf_to_df)?;

        Ok(CuDFGroupBy::from_table_view(table_view))
    }

    /// Evaluate each aggregate function's argument expressions on a batch,
    /// returning GPU column views suitable for `partial_requests` / `merge_requests`.
    ///
    /// Literal aggregate args, such as `COUNT(*)`, can evaluate to host Arrow arrays.
    /// Upload them here so aggregate requests always receive cuDF column views.
    fn evaluate_batch_arguments(&self, batch: &RecordBatch) -> Result<Vec<Vec<CuDFColumnView>>> {
        let _timer = self.group_by_metrics.aggregate_arguments_time.timer();
        let evaluated_arguments = evaluate_many(&self.aggregate_args, batch)?;
        let stream = record_batch_execution_stream(batch)
            .map_err(cudf_to_df)?
            .unwrap_or(global_execution_stream().map_err(cudf_to_df)?);

        evaluated_arguments
            .iter()
            .map(|args| {
                args.iter()
                    .map(|arg| {
                        if let Some(view) = arg.as_any().downcast_ref::<CuDFColumnView>() {
                            return Ok(view.clone());
                        }
                        CuDFColumn::try_from_arrow_host_on_stream(arg.as_ref(), &stream)
                            .map(|col| col.into_view())
                            .map_err(cudf_to_df)
                    })
                    .collect()
            })
            .collect()
    }
}

fn finalize_workers(
    mut workers: Vec<(CuDFStream, AggregateWorker)>,
    mut empty_worker: AggregateWorker,
) -> Result<Option<RecordBatch>> {
    for (stream, _) in &workers {
        stream.synchronize().map_err(cudf_to_df)?;
    }

    let Some(primary) = workers
        .iter()
        .position(|(_, worker)| worker.running.is_some())
    else {
        return empty_worker.build_output();
    };
    let (stream, mut worker) = workers.swap_remove(primary);

    for (_, mut other) in workers {
        let Some(running) = other.running.take() else {
            continue;
        };
        let running = copy_running_state_to_stream(running, &stream)?;
        worker.merge_into_running(running.keys, running.state_columns)?;
    }
    worker.build_output()
}

fn copy_running_state_to_stream(
    running: RunningState,
    stream: &CuDFStream,
) -> Result<RunningState> {
    let key_view = running.keys.into_view();
    let state_views = running
        .state_columns
        .into_iter()
        .map(CuDFColumn::into_view)
        .collect::<Vec<_>>();

    let keys = CuDFTable::concat_on_stream(vec![key_view], stream).map_err(cudf_to_df)?;
    let state_columns = state_views
        .into_iter()
        .map(|view| CuDFColumn::concat_on_stream(vec![view], stream).map_err(cudf_to_df))
        .collect::<Result<Vec<_>>>()?;

    stream.synchronize().map_err(cudf_to_df)?;

    Ok(RunningState {
        keys,
        state_columns,
    })
}

/// Concatenate CuDF-backed record batches into a single batch by column.
fn concat_cudf_batches(batches: &[RecordBatch]) -> Result<RecordBatch> {
    let schema = batches[0].schema();
    let num_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let cols = (0..schema.fields().len())
        .map(|i| {
            let views = batches
                .iter()
                .map(|b| {
                    let Some(view) = b.column(i).as_any().downcast_ref::<CuDFColumnView>() else {
                        return internal_err!(
                            "Expected Array to be of type CuDFColumnView after CuDFLoadExec"
                        );
                    };
                    Ok(view.clone())
                })
                .collect::<Result<Vec<_>>>()?;
            let col = CuDFColumn::concat(views).map_err(cudf_to_df)?;
            Ok(Arc::new(col.into_view()) as Arc<dyn Array>)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(record_batch_with_schema(cols, &schema, num_rows)?)
}

/// State transitions for one aggregate output partition.
///
/// ```text
///                         Running
///             poll input and route each batch
///              by its attached CUDA stream
///              /            |       ...       \
///             v             v                  v
///       lane 0 buffer  lane 1 buffer  ...  lane N buffer
///             |             |                  |
///       chunk aggregate chunk aggregate ... chunk aggregate
///             |             |                  |
///       lane 0 state   lane 1 state    ... lane N state
///              \            |       ...       /
///               \           |                /
///                  input exhausted
///                         |
///                         v
///                    Finalizing
///       synchronize lanes, copy the secondary state,
///       merge and finalize on the primary lane's stream
///                         |
///                         v
///                       Done
/// ```
enum StreamState {
    /// Read input and drive the independent per-stream aggregate workers.
    Running,
    /// Final casts or division kernels + constructing the single output batch.
    Finalizing(JoinHandle<Result<Option<RecordBatch>>>),
    /// No more work to do. All later polls return `None`.
    Done,
}

impl futures::Stream for CuDFAggregateStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                StreamState::Running => {
                    if let Err(error) = self.poll_lane_tasks(cx) {
                        self.state = StreamState::Done;
                        return Poll::Ready(Some(Err(error)));
                    }

                    let input_exhausted = self.input_exhausted;
                    if self.start_ready_lane_tasks(input_exhausted) {
                        continue;
                    }

                    if self.input_exhausted {
                        if self.lanes.iter().any(|lane| lane.task.is_some()) {
                            return Poll::Pending;
                        }
                        self.start_finalizing();
                        continue;
                    }

                    if self.is_backpressured() {
                        return Poll::Pending;
                    }

                    match self.input.poll_next_unpin(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(None) => self.input_exhausted = true,
                        Poll::Ready(Some(Err(error))) => {
                            self.state = StreamState::Done;
                            return Poll::Ready(Some(Err(error)));
                        }
                        Poll::Ready(Some(Ok(batch))) => {
                            let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
                            let _timer = elapsed_compute.timer();
                            if let Some(reduction) = self.reduction_factor.as_ref() {
                                reduction.add_total(batch.num_rows());
                            }
                            let batch_bytes = batch.get_array_memory_size();
                            let lane_index = match self.lane_for_batch(&batch) {
                                Ok(index) => index,
                                Err(error) => {
                                    self.state = StreamState::Done;
                                    return Poll::Ready(Some(Err(error)));
                                }
                            };
                            let lane = &mut self.lanes[lane_index];
                            lane.pending_bytes = lane.pending_bytes.saturating_add(batch_bytes);
                            lane.pending_batches.push(batch);
                        }
                    }
                }
                StreamState::Finalizing(task) => match Pin::new(task).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(Ok(output))) => {
                        self.state = StreamState::Done;
                        return match output {
                            Some(batch) => {
                                if let Some(reduction) = self.reduction_factor.as_ref() {
                                    reduction.add_part(batch.num_rows());
                                }
                                self.baseline_metrics.record_output(&batch);
                                Poll::Ready(Some(Ok(batch)))
                            }
                            None => Poll::Ready(None),
                        };
                    }
                    Poll::Ready(Ok(Err(error))) => {
                        self.state = StreamState::Done;
                        return Poll::Ready(Some(Err(error)));
                    }
                    Poll::Ready(Err(error)) => {
                        self.state = StreamState::Done;
                        return Poll::Ready(Some(Err(blocking_task_error(error))));
                    }
                },
                StreamState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for CuDFAggregateStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

fn blocking_task_error(error: JoinError) -> DataFusionError {
    DataFusionError::Execution(format!("CuDF aggregate blocking task failed: {error}"))
}
