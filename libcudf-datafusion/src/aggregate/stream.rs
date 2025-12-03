use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::error::Result;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use futures::{ready, StreamExt};
use std::pin::Pin;
use std::task::{Context, Poll};

enum State {
    ReceivingInput,
    Done,
}

pub struct Stream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    state: State,
}

impl futures::Stream for Stream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &self.state {
            State::ReceivingInput => {
                match ready!(self.input.poll_next_unpin(cx)) {
                    None => {
                        // finished
                        self.state = State::Done;
                        Poll::Ready(None)
                    }
                    Some(Err(err)) => Poll::Ready(Some(Err(err))),
                    Some(Ok(batch)) => Poll::Ready(Some(Ok(batch))),
                }
            }
            State::Done => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for Stream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
