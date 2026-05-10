use crate::metrics::CuDFBaselineMetrics;
use datafusion::common::{assert_eq_or_internal_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::Partitioning;
use datafusion_physical_plan::execution_plan::{CardinalityEffect, EvaluationType, SchedulingType};
use datafusion_physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion_physical_plan::stream::{RecordBatchReceiverStream, RecordBatchStreamAdapter};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    Statistics,
};
use futures_util::{Stream, StreamExt};
use std::any::Any;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Coalesces cuDF-backed partitions without host-side Arrow buffer inspection.
#[derive(Debug)]
pub(crate) struct CuDFCoalescePartitionsExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
    fetch: Option<usize>,
}

impl CuDFCoalescePartitionsExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let input_partitions = input.output_partitioning().partition_count();
        let (evaluation_type, scheduling_type) = if input_partitions > 1 {
            (EvaluationType::Eager, SchedulingType::Cooperative)
        } else {
            (
                input.properties().evaluation_type,
                input.properties().scheduling_type,
            )
        };
        let mut eq_properties = input.equivalence_properties().clone();
        eq_properties.clear_orderings();
        eq_properties.clear_per_partition_constants();
        let properties = Arc::new(
            PlanProperties::new(
                eq_properties,
                Partitioning::UnknownPartitioning(1),
                input.pipeline_behavior(),
                input.boundedness(),
            )
            .with_evaluation_type(evaluation_type)
            .with_scheduling_type(scheduling_type),
        );

        Self {
            input,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
            fetch: None,
        }
    }

    pub(crate) fn with_fetch(mut self, fetch: Option<usize>) -> Self {
        self.fetch = fetch;
        self
    }
}

impl DisplayAs for CuDFCoalescePartitionsExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => match self.fetch {
                Some(fetch) => write!(f, "CuDFCoalescePartitionsExec: fetch={fetch}"),
                None => write!(f, "CuDFCoalescePartitionsExec"),
            },
            DisplayFormatType::TreeRender => match self.fetch {
                Some(fetch) => write!(f, "limit: {fetch}"),
                None => write!(f, ""),
            },
        }
    }
}

impl ExecutionPlan for CuDFCoalescePartitionsExec {
    fn name(&self) -> &str {
        "CuDFCoalescePartitionsExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return plan_err!(
                "CuDFCoalescePartitionsExec expects exactly 1 child, {} were provided",
                children.len()
            );
        }
        Ok(Arc::new(
            Self::new(Arc::clone(&children[0])).with_fetch(self.fetch),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        assert_eq_or_internal_err!(
            partition,
            0,
            "CuDFCoalescePartitionsExec invalid partition {partition}"
        );

        let metrics = CuDFBaselineMetrics::new(&self.metrics, partition);
        if self.fetch == Some(0) {
            let stream =
                RecordBatchStreamAdapter::new(self.schema(), futures_util::stream::empty());
            return Ok(Box::pin(CuDFObservedStream {
                inner: Box::pin(stream),
                metrics,
                fetch: self.fetch,
                produced: 0,
            }));
        }

        let input_partitions = self.input.output_partitioning().partition_count();
        let stream = match input_partitions {
            0 => return plan_err!("CuDFCoalescePartitionsExec requires at least one partition"),
            1 => self.input.execute(0, context)?,
            _ => {
                let mut builder =
                    RecordBatchReceiverStream::builder(self.schema(), input_partitions);
                for input_partition in 0..input_partitions {
                    let input = Arc::clone(&self.input);
                    let context = Arc::clone(&context);
                    let output = builder.tx();
                    builder.spawn(async move {
                        let mut stream = match input.execute(input_partition, context) {
                            Ok(stream) => stream,
                            Err(error) => {
                                output.send(Err(error)).await.ok();
                                return Ok(());
                            }
                        };

                        while let Some(item) = stream.next().await {
                            let is_err = item.is_err();
                            if output.send(item).await.is_err() || is_err {
                                return Ok(());
                            }
                        }

                        Ok(())
                    });
                }
                builder.build()
            }
        };

        Ok(Box::pin(CuDFObservedStream {
            inner: stream,
            metrics,
            fetch: self.fetch,
            produced: 0,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(
        &self,
        _partition: Option<usize>,
    ) -> datafusion::common::Result<Statistics> {
        self.input
            .partition_statistics(None)?
            .with_fetch(self.fetch, 0, 1)
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        Some(Arc::new(
            Self::new(Arc::clone(&self.input)).with_fetch(limit),
        ))
    }
}

struct CuDFObservedStream {
    inner: SendableRecordBatchStream,
    metrics: CuDFBaselineMetrics,
    fetch: Option<usize>,
    produced: usize,
}

impl CuDFObservedStream {
    fn limit_reached(
        &mut self,
        poll: Poll<Option<Result<arrow::record_batch::RecordBatch, DataFusionError>>>,
    ) -> Poll<Option<Result<arrow::record_batch::RecordBatch, DataFusionError>>> {
        let Some(fetch) = self.fetch else {
            return poll;
        };

        if self.produced >= fetch {
            return Poll::Ready(None);
        }

        if let Poll::Ready(Some(Ok(batch))) = &poll {
            if self.produced + batch.num_rows() > fetch {
                let batch = batch.slice(0, fetch.saturating_sub(self.produced));
                self.produced += batch.num_rows();
                return Poll::Ready(Some(Ok(batch)));
            }
            self.produced += batch.num_rows();
        }

        poll
    }
}

impl RecordBatchStream for CuDFObservedStream {
    fn schema(&self) -> arrow_schema::SchemaRef {
        self.inner.schema()
    }
}

impl Stream for CuDFObservedStream {
    type Item = Result<arrow::record_batch::RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.fetch.is_some_and(|fetch| self.produced >= fetch) {
            return self.metrics.record_poll(Poll::Ready(None));
        }

        let mut poll = self.inner.poll_next_unpin(cx);
        if self.fetch.is_some() {
            poll = self.limit_reached(poll);
        }
        self.metrics.record_poll(poll)
    }
}
