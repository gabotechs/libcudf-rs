use crate::aggregate::GpuAggregateExpr;
use crate::errors::cudf_to_df;
use arrow::array::{Array, RecordBatch};
use arrow_schema::SchemaRef;
use datafusion::common::exec_datafusion_err;
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
    aggr_expr: Vec<Arc<GpuAggregateExpr>>,

    state: State,

    results: Vec<CuDFGroupByResult>,
}

impl Stream {
    pub fn new(
        input: SendableRecordBatchStream,
        output_schema: SchemaRef,
        group_by: PhysicalGroupBy,
        aggr_expr: Vec<Arc<GpuAggregateExpr>>,
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

                            let mut requests = Vec::with_capacity(evaluated_arguments.len());
                            for (agg, args) in self.aggr_expr.iter().zip(evaluated_arguments) {
                                requests.extend(agg.op.partial_requests(&args)?);
                            }

                            self.results
                                .push(group_by.aggregate(requests).map_err(cudf_to_df)?);
                        }
                    }
                }
                State::Final => {
                    // Final aggregation
                    let mut keys = Vec::with_capacity(self.results.len());

                    for result in &self.results {
                        keys.push(result.keys());
                    }

                    let keys_views = keys.into_iter().map(|x| x.into_inner()).collect::<Vec<_>>();
                    let groups = CuDFTable::from_ptr(ffi::concat_table_views(&keys_views));
                    let group_by = CuDFGroupBy::from(&groups.view());

                    let result = group_by.aggregate(vec![]).map_err(cudf_to_df)?;
                    let groups = result.keys().to_arrow_host().map_err(cudf_to_df)?;

                    self.state = State::Done;
                    return Poll::Ready(Some(Ok(groups)));
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
