use crate::aggregate::CuDFAggregateExpr;
use crate::errors::cudf_to_df;
use arrow::array::{ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use datafusion::common::exec_err;
use datafusion::error::Result;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_physical_plan::aggregates::{evaluate_group_by, evaluate_many, PhysicalGroupBy};
use futures::{ready, StreamExt};
use libcudf_rs::{ffi, CuDFColumnView, CuDFGroupBy, CuDFGroupByResult, CuDFTable, CuDFTableView};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

enum State {
    ReceivingInput,
    Final,
    Done,
}

pub struct Stream {
    input: SendableRecordBatchStream,
    output_schema: SchemaRef,

    group_by: PhysicalGroupBy,
    aggr_expr: Vec<CuDFAggregateExpr>,
    aggregate_arguments: Vec<Vec<Arc<dyn PhysicalExpr>>>,

    state: State,

    results: Vec<CuDFGroupByResult>,
}

impl Stream {
    pub fn new(
        input: SendableRecordBatchStream,
        output_schema: SchemaRef,
        group_by: PhysicalGroupBy,
        aggr_expr: Vec<CuDFAggregateExpr>,
    ) -> Self {
        let aggregate_arguments = aggr_expr
            .iter()
            .map(|x| x.arguments.clone())
            .collect::<Vec<_>>();

        Self {
            input,
            output_schema,
            group_by,
            aggr_expr,
            aggregate_arguments,
            state: State::ReceivingInput,
            results: vec![],
        }
    }
}

impl Stream {
    fn concat_keys(&self) -> Result<CuDFTable> {
        let mut keys = Vec::with_capacity(self.results.len());
        for result in &self.results {
            keys.push(result.keys());
        }
        let keys_views = keys.into_iter().map(|x| x.into_inner()).collect::<Vec<_>>();
        Ok(CuDFTable::from_ptr(ffi::concat_table_views(&keys_views)))
    }

    fn concat_partial_results(&self) -> Result<Vec<Vec<CuDFColumnView>>> {
        let first = self.results.first().expect("at least one result");
        let mut partial_columns = Vec::with_capacity(first.results_len());
        for index in 0..first.results_len() {
            let columns_len = first.columns_len(index);
            partial_columns.push(vec![Vec::with_capacity(self.results.len()); columns_len])
        }

        for result in &self.results {
            for result_index in 0..first.results_len() {
                for column_index in 0..first.columns_len(result_index) {
                    partial_columns[result_index][column_index]
                        .push(result.get_column(result_index, column_index))
                }
            }
        }

        let concatenated_columns = partial_columns
            .into_iter()
            .map(|columns| {
                columns
                    .into_iter()
                    .map(|views| {
                        let view_ptrs = views
                            .into_iter()
                            .map(|x| x.into_inner())
                            .collect::<Vec<_>>();
                        CuDFColumnView::from_column(ffi::concat_column_views(&view_ptrs))
                    })
                    .collect()
            })
            .collect::<Vec<Vec<_>>>();

        Ok(concatenated_columns)
    }

    fn build_final_batch(&mut self, mut result: CuDFGroupByResult) -> Result<RecordBatch> {
        let mut groups = result.take_keys().take();
        let mut arrays: Vec<ArrayRef> =
            Vec::with_capacity(groups.len() + self.aggr_expr.len());
        for i in 0..groups.len() {
            arrays.push(Arc::new(groups.release(i)))
        }

        for (result_index, aggr) in self.aggr_expr.iter().enumerate() {
            let args = (0..result.columns_len(result_index))
                .map(|columns_index| result.take_column(result_index, columns_index))
                .collect::<Vec<_>>();
            let merged = aggr.op.merge(&args)?;
            arrays.push(Arc::new(merged));
        }

        Ok(RecordBatch::try_new(self.output_schema.clone(), arrays)?)
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
                            let grouping_sets = evaluate_group_by(&self.group_by, &batch)?;
                            if grouping_sets.len() != 1 {
                                return Poll::Ready(Some(exec_err!(
                                    "Expected single grouping set, got {}",
                                    grouping_sets.len()
                                )));
                            }

                            let group = &grouping_sets[0];
                            let column_views = group
                                .into_iter()
                                .map(|x| x.as_any().downcast_ref::<CuDFColumnView>().unwrap())
                                .collect::<Vec<_>>();

                            let table_view = CuDFTableView::from_column_views(&column_views).map_err(cudf_to_df)?;
                            let group_by = CuDFGroupBy::from(&table_view);

                            let evaluated_arguments = evaluate_many(&self.aggregate_arguments, &batch)?;
                            let evaluated_views = evaluated_arguments
                                .iter()
                                .map(|args| {
                                    args.iter()
                                        .map(|arg| {
                                            CuDFColumnView::from_arrow(&arg).map_err(cudf_to_df)
                                        })
                                        .collect()
                                })
                                .collect::<Result<Vec<Vec<_>>>>()?;

                            let mut requests = Vec::with_capacity(evaluated_views.len());
                            for (agg, args) in self.aggr_expr.iter().zip(evaluated_views) {
                                requests.extend(agg.op.partial_requests(&args)?)
                            }

                            self.results
                                .push(group_by.aggregate(&requests).map_err(cudf_to_df)?);
                        }
                    }
                }
                State::Final => {
                    if self.results.is_empty() {
                        return Poll::Ready(None);
                    }

                    let keys_table = self.concat_keys()?;
                    let group_by = CuDFGroupBy::from(&keys_table.view());

                    let concatenated_columns = self.concat_partial_results()?;

                    let mut requests = Vec::with_capacity(self.aggr_expr.len());
                    for (agg, args) in self.aggr_expr.iter().zip(concatenated_columns.iter()) {
                        requests.extend(agg.op.final_requests(args)?);
                    }

                    let result = group_by.aggregate(&requests).map_err(cudf_to_df)?;
                    let output = self.build_final_batch(result)?;

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
