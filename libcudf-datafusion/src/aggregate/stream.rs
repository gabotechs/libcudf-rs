use crate::aggregate::GpuAggregateExpr;
use crate::errors::cudf_to_df;
use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::error::Result;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
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
    aggr_expr: Vec<GpuAggregateExpr>,

    state: State,

    results: Vec<CuDFGroupByResult>,
}

impl Stream {
    pub fn new(
        input: SendableRecordBatchStream,
        output_schema: SchemaRef,
        group_by: PhysicalGroupBy,
        aggr_expr: Vec<GpuAggregateExpr>,
    ) -> Self {
        Self {
            input,
            output_schema,
            group_by,
            aggr_expr,
            state: State::ReceivingInput,
            results: vec![],
        }
    }
}

impl futures::Stream for Stream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let aggregate_arguments = self
            .aggr_expr
            .iter()
            .map(|x| x.arguments.clone())
            .collect::<Vec<_>>();

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
                            assert_eq!(grouping_sets.len(), 1);

                            let group = &grouping_sets[0];
                            let column_views = group
                                .into_iter()
                                .map(|x| x.as_any().downcast_ref::<CuDFColumnView>().unwrap())
                                .collect::<Vec<_>>();

                            let table_view = CuDFTableView::from_column_views(&column_views).map_err(cudf_to_df)?;
                            let group_by = CuDFGroupBy::from(&table_view);

                            let evaluated_arguments = evaluate_many(&aggregate_arguments, &batch)?;
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
                    if self.results.len() == 0 {
                        return Poll::Ready(None);
                    }

                    let mut keys = Vec::with_capacity(self.results.len());

                    for result in &self.results {
                        keys.push(result.keys());
                    }

                    let keys_views = keys.into_iter().map(|x| x.into_inner()).collect::<Vec<_>>();
                    let groups = CuDFTable::from_ptr(ffi::concat_table_views(&keys_views));
                    let group_by = CuDFGroupBy::from(&groups.view());

                    let first = self.results.first().expect("results should be > 1");
                    let mut views = Vec::with_capacity(first.results_len());
                    for index in 0..first.results_len() {
                        let columns_len = first.columns_len(index);
                        views.push(vec![Vec::with_capacity(self.results.len()); columns_len])
                    }

                    for result in &self.results {
                        for result_index in 0..first.results_len() {
                            for column_index in 0..first.columns_len(result_index) {
                                views[result_index][column_index]
                                    .push(result.column(result_index, column_index))
                            }
                        }
                    }

                    let columns = views
                        .into_iter()
                        .map(|views| {
                            views
                                .into_iter()
                                .map(|views| {
                                    let views = views
                                        .into_iter()
                                        .map(|x| x.into_inner())
                                        .collect::<Vec<_>>();
                                    CuDFColumnView::from_column(ffi::concat_column_views(&views))
                                })
                                .collect()
                        })
                        .collect::<Vec<Vec<_>>>();

                    let mut requests = Vec::with_capacity(self.aggr_expr.len()); // lower bound
                    for (agg, args) in self.aggr_expr.iter().zip(columns.iter()) {
                        requests.extend(agg.op.final_requests(args)?);
                    }

                    let result = group_by.aggregate(&requests).map_err(cudf_to_df)?;

                    let groups = result.keys().to_arrow_host().map_err(cudf_to_df)?;

                    let mut arrays = groups.columns().to_vec();
                    arrays.reserve(self.aggr_expr.len());

                    for (result_index, aggr) in self.aggr_expr.iter().enumerate() {
                        let args = (0..result.columns_len(result_index))
                            .map(|columns_index| result.column(result_index, columns_index))
                            .collect::<Vec<_>>();
                        let merged = aggr.op.merge(&args)?;
                        arrays.push(Arc::new(merged));
                    }

                    let output = RecordBatch::try_new(self.output_schema.clone(), arrays)?;

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
