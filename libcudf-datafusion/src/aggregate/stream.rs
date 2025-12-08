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

pub struct Stream {
    input: SendableRecordBatchStream,
    output_schema: SchemaRef,

    group_by: PhysicalGroupBy,

    aggregate_expr: Vec<Arc<AggregateFunctionExpr>>,
    aggregate_args: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    aggregate_ops: Vec<Arc<dyn CuDFAggregationOp>>,

    state: State,

    keys: Vec<CuDFTable>,
    results: Vec<Vec<Vec<CuDFColumn>>>,
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
        }
    }
}

impl Stream {
    fn concat_keys(&mut self) -> Result<CuDFTable> {
        let mut keys = Vec::with_capacity(self.keys.len());
        for table in std::mem::take(&mut self.keys) {
            keys.push(table.into_view());
        }
        CuDFTable::concat(keys).map_err(cudf_to_df)
    }

    fn concat_partial_results(&mut self) -> Result<Vec<Vec<CuDFColumnView>>> {
        let first = self.results.first().expect("at least one result");
        let mut partial_columns = Vec::with_capacity(first.len());
        for agg_results in first {
            partial_columns.push(vec![
                Vec::with_capacity(self.results.len());
                agg_results.len()
            ])
        }

        for results in std::mem::take(&mut self.results) {
            for (r_i, columns) in results.into_iter().enumerate() {
                for (c_i, column) in columns.into_iter().enumerate() {
                    partial_columns[r_i][c_i].push(column.into_view());
                }
            }
        }

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

    fn build_final_batch(
        &self,
        keys: CuDFTable,
        results: Vec<Vec<CuDFColumn>>,
    ) -> Result<RecordBatch> {
        let groups = keys.into_columns();
        let mut arrays: Vec<ArrayRef> =
            Vec::with_capacity(groups.len() + self.aggregate_expr.len());
        for group in groups {
            arrays.push(Arc::new(group.into_view()))
        }

        for (r_i, result) in results.into_iter().enumerate() {
            let aggr = &self.aggregate_ops[r_i];
            let args = result
                .into_iter()
                .map(|c| c.into_view())
                .collect::<Vec<_>>();
            let merged = aggr.merge(&args)?;
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
                                .cloned()
                                .collect::<Vec<_>>();

                            let table_view = CuDFTableView::from_column_views(column_views)
                                .map_err(cudf_to_df)?;
                            let group_by = CuDFGroupBy::from(&table_view);

                            let evaluated_arguments = evaluate_many(&self.aggregate_args, &batch)?;
                            let evaluated_views = evaluated_arguments
                                .iter()
                                .map(|args| {
                                    args.iter()
                                        .map(|arg| {
                                            let Some(view) =
                                                arg.as_any().downcast_ref::<CuDFColumnView>()
                                            else {
                                                return internal_err!(
                                                    "Expected Array to be of type CuDFColumnView"
                                                );
                                            };
                                            Ok(view.clone())
                                        })
                                        .collect()
                                })
                                .collect::<Result<Vec<Vec<_>>>>()?;

                            let mut requests = Vec::with_capacity(evaluated_views.len());
                            for (agg, args) in self.aggregate_ops.iter().zip(evaluated_views) {
                                requests.extend(agg.partial_requests(&args)?)
                            }
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
                    let group_by = CuDFGroupBy::from(&keys_table.into_view());

                    let concatenated_columns = self.concat_partial_results()?;

                    let mut requests = Vec::with_capacity(self.aggregate_expr.len());
                    for (agg, args) in self.aggregate_ops.iter().zip(concatenated_columns.iter()) {
                        requests.extend(agg.final_requests(args)?);
                    }

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
