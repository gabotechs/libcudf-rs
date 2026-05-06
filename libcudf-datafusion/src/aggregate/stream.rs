use crate::aggregate::PreparedCuDFAggregate;
use crate::errors::cudf_to_df;
use crate::metrics::CuDFBaselineMetrics;
use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use datafusion::common::{exec_err, internal_err};
use datafusion::error::Result;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion::physical_expr_common::metrics::{
    ExecutionPlanMetricsSet, MetricBuilder, MetricType, RatioMetrics, Time,
};
use datafusion_physical_plan::aggregates::{evaluate_group_by, evaluate_many, AggregateMode};
use datafusion_physical_plan::PhysicalExpr;
use futures::{ready, StreamExt};
use libcudf_rs::{
    record_batch_with_schema, CuDFColumn, CuDFColumnView, CuDFGroupBy, CuDFTable, CuDFTableView,
};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

enum StreamState {
    ReadingInput,
    ProducingOutput,
    Done,
}

/// Aggregate-specific timers. Mirrors upstream `GroupByMetrics` from
/// `datafusion::physical_plan::aggregates::group_values::metrics`: same
/// field names and same `subset_time` metric keys, so EXPLAIN ANALYZE
/// looks identical to a CPU `AggregateExec`.
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

/// Maps each aggregation op to its slice of the flat state_columns array.
///
/// Built once at construction from `num_state_columns()`.
///
/// Example for `[SUM, AVG, MAX]`:
/// ```text
///   SUM -> (0, 1), AVG -> (1, 2), MAX -> (3, 1)
/// ```
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
/// Input batches are accumulated into a pending buffer. Once the buffer reaches
/// the configured byte budget (or input is exhausted), pending batches are
/// concatenated on the GPU into a single table and aggregated in one kernel
/// call. The result (at most G rows, where G is the group cardinality) is merged
/// into the running state using the standard partial-state merge.
///
/// After all input is consumed, a single output batch is produced by either
/// finalizing the state (Single/Final modes) or emitting raw state columns
/// (Partial mode).
pub struct CuDFAggregateStream {
    input: SendableRecordBatchStream,
    output_schema: SchemaRef,
    prepared: PreparedCuDFAggregate,
    /// cuDF expressions that extract each op's argument columns from a batch.
    aggregate_args: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    column_mapping: ColumnMapping,
    state: StreamState,
    /// Batches accumulated since the last flush, cleared on each flush.
    pending_batches: Vec<RecordBatch>,
    /// Estimated GPU memory held by `pending_batches`.
    pending_bytes: usize,
    /// Target input bytes accumulated before running an aggregate/merge cycle.
    chunk_target_bytes: usize,
    /// Aggregated running state (at most G rows), updated after each flush.
    running: Option<RunningState>,
    /// Output rows/bytes/batches + total elapsed_compute, GPU-safe.
    baseline_metrics: CuDFBaselineMetrics,
    /// Per-stage timers, named to match upstream `AggregateExec`.
    group_by_metrics: GroupByMetrics,
    /// Partial-mode-only ratio of input rows to output rows. `None` for
    /// `Single`/`Final*` modes where the metric is not meaningful.
    reduction_factor: Option<RatioMetrics>,
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
                .with_type(MetricType::SUMMARY)
                .ratio_metrics("reduction_factor", partition)
        });

        Ok(Self {
            input,
            output_schema,
            prepared,
            aggregate_args,
            column_mapping,
            state: StreamState::ReadingInput,
            pending_batches: Vec::new(),
            pending_bytes: 0,
            chunk_target_bytes: chunk_target_bytes.max(1),
            running: None,
            baseline_metrics,
            group_by_metrics,
            reduction_factor,
        })
    }

    /// Aggregate all pending batches in one GPU kernel call and merge the result
    /// into the running state.
    ///
    /// Clears `pending_batches` on return.
    fn flush_pending(&mut self) -> Result<()> {
        if self.pending_batches.is_empty() {
            return Ok(());
        }

        let chunk = concat_cudf_batches(&self.pending_batches)?;
        self.pending_batches.clear();
        self.pending_bytes = 0;

        let group_by = self.evaluate_batch_groups(&chunk)?;
        let evaluated_args = self.evaluate_batch_arguments(&chunk)?;
        let requests = self.build_batch_requests(evaluated_args)?;

        let (chunk_keys, chunk_results) = {
            let _timer = self.group_by_metrics.aggregation_time.timer();
            group_by.aggregate(&requests).map_err(cudf_to_df)?
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
        for (run_col, new_col) in running
            .state_columns
            .into_iter()
            .zip(new_state_columns.into_iter())
        {
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
            group_by.aggregate(&requests).map_err(cudf_to_df)?
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
        let key_columns = running.keys.into_columns();
        let mut arrays: Vec<ArrayRef> =
            Vec::with_capacity(key_columns.len() + self.prepared.aggs.len());

        for col in key_columns {
            arrays.push(Arc::new(col.into_view()));
        }

        let state_views: Vec<CuDFColumnView> = running
            .state_columns
            .into_iter()
            .map(|c| c.into_view())
            .collect();

        let is_partial = matches!(self.prepared.mode, AggregateMode::Partial);

        for (op_idx, agg) in self.prepared.aggs.iter().enumerate() {
            let (start, count) = self.column_mapping.ranges[op_idx];
            let state_views: Vec<CuDFColumnView> = state_views[start..start + count].to_vec();

            if is_partial {
                // Partial mode: emit raw state columns, cast to match state_fields schema
                let state_fields = agg.expr.state_fields()?;
                for (col_idx, view) in state_views.into_iter().enumerate() {
                    let target_type = state_fields[col_idx].data_type();
                    if view.data_type() != target_type {
                        let casted = libcudf_rs::cast(&view, target_type).map_err(cudf_to_df)?;
                        arrays.push(Arc::new(casted.into_view()));
                    } else {
                        arrays.push(Arc::new(view));
                    }
                }
            } else {
                // Single/Final/FinalPartitioned/SinglePartitioned: finalize
                let finalized = agg.op.finalize(&state_views, &agg.output_type)?;
                arrays.push(Arc::new(finalized));
            }
        }

        Ok(Some(record_batch_with_schema(
            arrays,
            &self.output_schema,
            num_rows,
        )?))
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

        let table_view = CuDFTableView::from_column_views(column_views).map_err(cudf_to_df)?;

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

        evaluated_arguments
            .iter()
            .map(|args| {
                args.iter()
                    .map(|arg| {
                        if let Some(view) = arg.as_any().downcast_ref::<CuDFColumnView>() {
                            return Ok(view.clone());
                        }
                        CuDFColumn::from_arrow_host(arg.as_ref())
                            .map(|col| col.into_view())
                            .map_err(cudf_to_df)
                    })
                    .collect()
            })
            .collect()
    }
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

impl futures::Stream for CuDFAggregateStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &self.state {
                StreamState::ReadingInput => match ready!(self.input.poll_next_unpin(cx)) {
                    None => {
                        self.state = StreamState::ProducingOutput;
                    }
                    Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                    Some(Ok(batch)) => {
                        // Don't include `input.poll_next_unpin` wait time here.
                        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
                        let _timer = elapsed_compute.timer();
                        if let Some(reduction) = self.reduction_factor.as_ref() {
                            reduction.add_total(batch.num_rows());
                        }
                        self.pending_bytes = self
                            .pending_bytes
                            .saturating_add(batch.get_array_memory_size());
                        self.pending_batches.push(batch);
                        if self.pending_bytes >= self.chunk_target_bytes {
                            self.flush_pending()?;
                        }
                    }
                },
                StreamState::ProducingOutput => {
                    let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
                    let _timer = elapsed_compute.timer();
                    self.flush_pending()?;
                    let output = self.build_output()?;
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
