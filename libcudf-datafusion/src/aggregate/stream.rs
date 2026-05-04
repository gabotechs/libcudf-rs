use crate::aggregate::op::udf::CuDFAggregateUDF;
use crate::aggregate::CuDFAggregationOp;
use crate::errors::cudf_to_df;
use arrow::array::{ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use datafusion::common::{exec_err, internal_err};
use datafusion::error::Result;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_physical_plan::aggregates::{evaluate_group_by, evaluate_many, PhysicalGroupBy};
use datafusion_physical_plan::udaf::AggregateFunctionExpr;
use futures::{ready, StreamExt};
use libcudf_rs::{CuDFColumn, CuDFColumnView, CuDFGroupBy, CuDFTable, CuDFTableView};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

enum State {
    ReceivingInput,
    Final,
    Done,
}

/// Results from partial aggregations across batches: `batches -> result groups -> columns`
/// - Outer Vec: Batches (one entry per input batch processed)
/// - Middle Vec: Result groups (one per aggregation request sent to cuDF)
/// - Inner Vec: Columns in that result group (typically 1, but >=2 for multi-column ops like AVG)
///
/// Use `column_mapping` to determine which result groups belong to which operation.
type PartialResults = Vec<Vec<Vec<CuDFColumn>>>;

/// Tracks which result groups belong to each aggregation operation.
///
/// Enables correct slicing of multi-column aggregations like AVG (uses sum and count).
///
/// Example for `[SUM, AVG, MAX]`:
/// ```text
/// column_mapping = [
///   ColumnRange { start: 0, count: 1 }, -> SUM uses result group 0
///   ColumnRange { start: 1, count: 2 }, -> AVG uses result groups 1-2
///   ColumnRange { start: 3, count: 1 }, -> MAX uses result group 3
/// ]
/// ```
#[derive(Debug, Clone)]
struct ColumnRange {
    start: usize, // Starting index in result groups
    count: usize, // Number of result groups this operation uses
}

pub struct Stream {
    input: SendableRecordBatchStream,
    output_schema: SchemaRef,

    group_by: PhysicalGroupBy,

    aggregate_expr: Vec<Arc<AggregateFunctionExpr>>,
    aggregate_args: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    aggregate_ops: Vec<Arc<dyn CuDFAggregationOp>>,

    state: State,

    keys: Vec<CuDFTable>,

    results: PartialResults,

    column_mapping: Vec<ColumnRange>,
}

impl Stream {
    pub fn new(
        input: SendableRecordBatchStream,
        output_schema: SchemaRef,
        group_by: PhysicalGroupBy,
        aggregate_expr: Vec<Arc<AggregateFunctionExpr>>,
    ) -> Self {
        let aggregate_args = aggregate_expr
            .iter()
            .map(|x| x.expressions())
            .collect::<Vec<_>>();

        let aggregate_ops = aggregate_expr
            .iter()
            .map(|x| {
                x.fun()
                    .inner()
                    .as_any()
                    .downcast_ref::<CuDFAggregateUDF>()
                    .expect("aggregate expr should be CuDFAggregateUDF")
                    .gpu()
                    .clone()
            })
            .collect::<Vec<_>>();

        Self {
            input,
            output_schema,
            group_by,
            aggregate_expr,
            aggregate_args,
            aggregate_ops,
            state: State::ReceivingInput,
            keys: vec![],
            results: vec![],
            column_mapping: vec![],
        }
    }

    /// Concatenate unique group keys from all batches.
    ///
    /// Consumes `self.keys` and produces a single table containing all unique group keys that will
    /// be used for the final aggregation phase.
    fn concat_keys(&mut self) -> Result<CuDFTable> {
        let mut keys = Vec::with_capacity(self.keys.len());
        for table in std::mem::take(&mut self.keys) {
            keys.push(table.into_view());
        }
        CuDFTable::concat(keys).map_err(cudf_to_df)
    }

    /// Concatenate partial aggregation results from all batches.
    ///
    /// This groups columns by their result group index (middle Vec in PartialResults),
    /// concatenates across batches, then returns them in the same grouped structure.
    ///
    /// # Steps
    /// 1. Group columns by result group index across all batches
    /// 2. For each result group, concatenate columns from all batches
    /// 3. Return concatenated results in same structure as input
    ///
    /// # Example
    /// ```text
    /// Input (3 batches, 2 result groups each):
    /// [
    ///   [[col_b1_g0], [col_b1_g1]], -> Batch 1
    ///   [[col_b2_g0], [col_b2_g1]], -> Batch 2
    ///   [[col_b3_g0], [col_b3_g1]]  -> Batch 3
    /// ]
    ///
    /// Output (2 result groups, concatenated across batches):
    /// [
    ///   [concat(col_b1_g0, col_b2_g0, col_b3_g0)], -> Group 0
    ///   [concat(col_b1_g1, col_b2_g1, col_b3_g1)]  -> Group 1
    /// ]
    /// ```
    fn concat_partial_results(&mut self) -> Result<Vec<Vec<CuDFColumnView>>> {
        // Infer structure from first batch
        let first = self.results.first().expect("at least one result");
        let mut partial_columns = Vec::with_capacity(first.len());
        for agg_results in first {
            partial_columns.push(vec![
                Vec::with_capacity(self.results.len());
                agg_results.len()
            ])
        }

        // Group columns by result group index across batches
        for results in std::mem::take(&mut self.results) {
            for (r_i, columns) in results.into_iter().enumerate() {
                for (c_i, column) in columns.into_iter().enumerate() {
                    partial_columns[r_i][c_i].push(column.into_view());
                }
            }
        }

        // Concatenate each group
        partial_columns
            .into_iter()
            .map(|columns| {
                columns
                    .into_iter()
                    .map(|views| Ok(CuDFColumn::concat(views).map_err(cudf_to_df)?.into_view()))
                    .collect()
            })
            .collect()
    }

    /// Build the final output RecordBatch from aggregation results.
    ///
    /// Combines group keys with merged aggregation results into a single RecordBatch
    /// matching the output schema.
    ///
    /// # Steps
    /// 1. Add group key columns to output
    /// 2. For each aggregation operation, extract its columns using column_mapping
    /// 3. Call merge() to combine multi-column results (e.g., AVG: sum/count → average)
    /// 4. Add merged result to output
    fn build_final_batch(
        &self,
        keys: CuDFTable,
        mut results: Vec<Vec<CuDFColumn>>,
    ) -> Result<RecordBatch> {
        let groups = keys.into_columns();
        let mut arrays: Vec<ArrayRef> =
            Vec::with_capacity(groups.len() + self.aggregate_expr.len());
        for group in groups {
            arrays.push(Arc::new(group.into_view()))
        }

        for (aggr, mapping) in self.aggregate_ops.iter().zip(&self.column_mapping) {
            // Use mapping to extract the correct columns for this operation
            let range = mapping.start..(mapping.start + mapping.count);
            let mut args: Vec<CuDFColumnView> = Vec::new();
            for i in range {
                for col in results[i].drain(..) {
                    args.push(col.into_view());
                }
            }
            let merged = aggr.merge(&args)?;
            arrays.push(Arc::new(merged));
        }

        Ok(RecordBatch::try_new(self.output_schema.clone(), arrays)?)
    }

    /// Evaluate group keys from a batch and create a CuDFGroupBy instance.
    ///
    /// Extracts the group key columns from the input batch and prepares them
    /// for use in GPU aggregation.
    fn evaluate_batch_groups(&self, batch: &RecordBatch) -> Result<CuDFGroupBy> {
        let grouping_sets = evaluate_group_by(&self.group_by, batch)?;

        if grouping_sets.len() != 1 {
            return exec_err!("Expected single grouping set, got {}", grouping_sets.len());
        }

        let group = &grouping_sets[0];
        let column_views = group
            .iter()
            .map(|x| x.as_any().downcast_ref::<CuDFColumnView>().unwrap())
            .cloned()
            .collect::<Vec<_>>();

        let table_view = CuDFTableView::from_column_views(column_views).map_err(cudf_to_df)?;

        Ok(CuDFGroupBy::from_table_view(table_view))
    }

    /// Evaluate aggregate function arguments from a batch.
    ///
    /// Extracts and validates the argument columns for each aggregation operation,
    /// ensuring they are CuDFColumnViews suitable for GPU processing.
    fn evaluate_batch_arguments(&self, batch: &RecordBatch) -> Result<Vec<Vec<CuDFColumnView>>> {
        let evaluated_arguments = evaluate_many(&self.aggregate_args, batch)?;

        evaluated_arguments
            .iter()
            .map(|args| {
                args.iter()
                    .map(|arg| {
                        let Some(view) = arg.as_any().downcast_ref::<CuDFColumnView>() else {
                            return internal_err!("Expected Array to be of type CuDFColumnView");
                        };
                        Ok(view.clone())
                    })
                    .collect()
            })
            .collect()
    }

    /// Build aggregation requests for partial phase.
    ///
    /// On first call, also computes and stores the column mapping that describes
    /// which result groups belong to which operations. On subsequent calls, reuses
    /// the existing mapping.
    ///
    /// Returns the list of aggregation requests to send to cuDF.
    fn build_partial_requests(
        &mut self,
        evaluated_views: Vec<Vec<CuDFColumnView>>,
    ) -> Result<Vec<libcudf_rs::AggregationRequest>> {
        let mut requests = Vec::with_capacity(evaluated_views.len());

        let is_first_batch = self.column_mapping.is_empty();

        if is_first_batch {
            // First batch: build mapping
            let mut col_offset = 0;
            let mut column_mapping = Vec::with_capacity(self.aggregate_ops.len());

            for (agg, args) in self.aggregate_ops.iter().zip(evaluated_views) {
                let op_requests = agg.partial_requests(&args)?;
                let col_count = op_requests.len();

                column_mapping.push(ColumnRange {
                    start: col_offset,
                    count: col_count,
                });

                col_offset += col_count;
                requests.extend(op_requests);
            }

            self.column_mapping = column_mapping;
        } else {
            // Subsequent batches: reuse mapping
            for (agg, args) in self.aggregate_ops.iter().zip(evaluated_views) {
                requests.extend(agg.partial_requests(&args)?);
            }
        }

        Ok(requests)
    }

    /// Build aggregation requests for final phase.
    ///
    /// Uses the column mapping to correctly slice concatenated partial results,
    /// ensuring each operation receives all its columns (e.g., AVG gets both sum and count).
    fn build_final_requests(
        &self,
        concatenated_columns: &[Vec<CuDFColumnView>],
    ) -> Result<Vec<libcudf_rs::AggregationRequest>> {
        let mut requests = Vec::with_capacity(self.aggregate_expr.len());

        for (agg, mapping) in self.aggregate_ops.iter().zip(&self.column_mapping) {
            // Slice to get columns for this operation
            let range = mapping.start..(mapping.start + mapping.count);
            let op_columns: Vec<CuDFColumnView> = concatenated_columns[range]
                .iter()
                .flat_map(|cols| cols.iter().cloned())
                .collect();

            requests.extend(agg.final_requests(&op_columns)?);
        }

        Ok(requests)
    }
}

impl futures::Stream for Stream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &self.state {
                State::ReceivingInput => {
                    match ready!(self.input.poll_next_unpin(cx)) {
                        None => {
                            // finished
                            self.state = State::Final;
                        }
                        Some(Err(err)) => return Poll::Ready(Some(Err(err))),
                        Some(Ok(batch)) => {
                            let group_by = self.evaluate_batch_groups(&batch)?;
                            let evaluated_views = self.evaluate_batch_arguments(&batch)?;
                            let requests = self.build_partial_requests(evaluated_views)?;

                            let (keys, results) =
                                group_by.aggregate(&requests).map_err(cudf_to_df)?;

                            self.results.push(results);
                            self.keys.push(keys);
                        }
                    }
                }
                State::Final => {
                    if self.results.is_empty() {
                        return Poll::Ready(None);
                    }

                    let keys_table = self.concat_keys()?;
                    let group_by = CuDFGroupBy::from_table_view(keys_table.into_view());

                    let concatenated_columns = self.concat_partial_results()?;
                    let requests = self.build_final_requests(&concatenated_columns)?;

                    let (keys, results) = group_by.aggregate(&requests).map_err(cudf_to_df)?;
                    let output = self.build_final_batch(keys, results)?;

                    self.state = State::Done;
                    return Poll::Ready(Some(Ok(output)));
                }
                State::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for Stream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}
