use crate::errors::cudf_to_df;
use arrow::array::RecordBatch;
use datafusion::common::Statistics;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::LexOrdering;
use datafusion_physical_plan::execution_plan::CardinalityEffect;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    execute_stream, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use delegate::delegate;
use futures::Stream;
use futures_util::{ready, StreamExt};
use libcudf_rs::CuDFTable;
use std::any::Any;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct CuDFSortExec {
    inner: SortExec,
}

impl CuDFSortExec {
    pub fn try_new(inner: SortExec) -> Result<Self, DataFusionError> {
        Ok(Self { inner })
    }
}

impl DisplayAs for CuDFSortExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CuDF")?;
        self.inner.fmt_as(t, f)
    }
}

impl ExecutionPlan for CuDFSortExec {
    fn name(&self) -> &str {
        "CuDFSortExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let inner = SortExec::new(self.inner.expr().clone(), children.swap_remove(0))
            .with_fetch(self.inner.fetch())
            .with_preserve_partitioning(self.inner.preserve_partitioning());
        Ok(Arc::new(Self::try_new(inner)?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let input = if self.inner.preserve_partitioning() {
            self.inner.input().execute(partition, context)?
        } else {
            execute_stream(Arc::clone(self.inner.input()), context)?
        };

        let ordered_stream = if let Some(limit) = self.fetch() {
            CuDFTopKStream {
                input,
                limit,
                ordering: self.inner.expr().clone(),
                result: None,
                finished: false,
            }
            .left_stream()
        } else {
            CuDFSortStream {
                input,
                ordering: self.inner.expr().clone(),
                batches: vec![],
                finished: false,
            }
            .right_stream()
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            ordered_stream,
        )))
    }

    delegate! {
        to self.inner {
            fn properties(&self) -> &PlanProperties;
            fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>>;
            fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics, DataFusionError>;
            fn fetch(&self) -> Option<usize>;
            fn cardinality_effect(&self) -> CardinalityEffect;
        }
    }
}

struct CuDFSortStream {
    input: SendableRecordBatchStream,
    ordering: LexOrdering,
    batches: Vec<RecordBatch>,
    finished: bool,
}

impl Stream for CuDFSortStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                self.batches.push(batch);
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Some(Err(err)) => Poll::Ready(Some(Err(err))),
            None => {
                self.finished = true;
                // TODO: Here, we need to sort self.batches and emit the final batch already sorted.
                todo!()
            }
        }
    }
}

struct CuDFTopKStream {
    input: SendableRecordBatchStream,
    ordering: LexOrdering,
    limit: usize,
    result: Option<CuDFTable>,
    finished: bool,
}

impl Stream for CuDFTopKStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                // TODO: Here, we need to accumulate the TopK in self.result using CuDF operations.
                todo!()
            }
            Some(Err(err)) => Poll::Ready(Some(Err(err))),
            None => {
                self.finished = true;
                let Some(result) = self.result.take() else {
                    return Poll::Ready(None);
                };
                let result = result.into_view().to_record_batch().map_err(cudf_to_df);
                Poll::Ready(Some(result))
            }
        }
    }
}
