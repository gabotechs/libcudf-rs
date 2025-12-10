use crate::errors::cudf_to_df;
use arrow::array::{Array, RecordBatch};
use datafusion::common::{internal_err, Statistics};
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
use libcudf_rs::{
    gather, slice_column, sort_by_all, stable_sorted_order, CuDFTable, CuDFTableView, SortOrder,
};
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
                views: vec![],
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
    views: Vec<CuDFTableView>,
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
                let view = CuDFTableView::from_record_batch(&batch).map_err(cudf_to_df)?;
                self.views.push(view);
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Some(Err(err)) => Poll::Ready(Some(Err(err))),
            None => {
                self.finished = true;

                if self.views.is_empty() {
                    return Poll::Ready(None);
                }

                let views = self.views.drain(..).collect();
                let concatenated = CuDFTable::concat(views).map_err(cudf_to_df)?;
                let table_view = concatenated.into_view();

                let sort_orders = extract_sort_params(&self.ordering, table_view.num_columns());
                let sorted = sort_by_all(&table_view, &sort_orders).map_err(cudf_to_df)?;

                let result = sorted.into_view().to_record_batch().map_err(cudf_to_df);

                Poll::Ready(Some(result))
            }
        }
    }
}

struct CuDFTopKStream {
    input: SendableRecordBatchStream,
    ordering: LexOrdering,
    limit: usize,
    result: Option<CuDFTableView>,
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
                let view = CuDFTableView::from_record_batch(&batch).map_err(cudf_to_df)?;

                let merged_table = if let Some(existing) = self.result.take() {
                    let views = vec![existing, view];
                    CuDFTable::concat(views).map_err(cudf_to_df)?
                } else {
                    CuDFTable::concat(vec![view]).map_err(cudf_to_df)?
                };

                self.result = Some(merged_table.into_view());
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Some(Err(err)) => Poll::Ready(Some(Err(err))),
            None => {
                self.finished = true;
                let Some(result) = self.result.take() else {
                    return Poll::Ready(None);
                };

                // Top-K implementation using stable_sorted_order + slice + gather
                // Note: This is O(N log N) as cuDF doesn't provide a heap-based O(N*k) top-K
                // for multi-column lexicographic ordering. See: https://github.com/rapidsai/cudf/pull/19303
                let sort_orders = extract_sort_params(&self.ordering, result.num_columns());

                // Get sorted indices
                let indices = stable_sorted_order(&result, &sort_orders).map_err(cudf_to_df)?;

                // Slice to keep only top K indices
                let indices_view = Arc::new(indices).view();
                let topk_indices_view =
                    slice_column(&indices_view, 0, self.limit).map_err(cudf_to_df)?;

                // Gather the top K rows
                let topk_table = gather(&result, &topk_indices_view).map_err(cudf_to_df)?;

                let batch = topk_table
                    .into_view()
                    .to_record_batch()
                    .map_err(cudf_to_df)?;
                Poll::Ready(Some(Ok(batch)))
            }
        }
    }
}

fn extract_sort_params(ordering: &LexOrdering, num_columns: usize) -> Vec<SortOrder> {
    let mut sort_orders: Vec<SortOrder> = ordering
        .iter()
        .map(
            |expr| match (expr.options.descending, expr.options.nulls_first) {
                (false, true) => SortOrder::AscendingNullsFirst,
                (false, false) => SortOrder::AscendingNullsLast,
                (true, true) => SortOrder::DescendingNullsFirst,
                (true, false) => SortOrder::DescendingNullsLast,
            },
        )
        .collect();

    // Pad with default sort order for remaining columns (they won't affect the sort)
    while sort_orders.len() < num_columns {
        sort_orders.push(SortOrder::AscendingNullsLast);
    }

    sort_orders
}

#[cfg(test)]
mod tests {
    use crate::assert_snapshot;
    use crate::test_utils::TestFramework;

    #[tokio::test]
    async fn test_basic_sort() -> Result<(), Box<dyn std::error::Error>> {
        let tf = TestFramework::new().await;

        let host_sql = r#"
            SELECT "MinTemp", "MaxTemp"
            FROM weather
            ORDER BY "MinTemp" ASC
        "#;
        let cudf_sql = format!(r#" SET cudf.enable=true; {host_sql} "#);

        let plan = tf.plan(&cudf_sql).await?;
        assert_snapshot!(plan.display(), @r"
        SortPreservingMergeExec: [MinTemp@0 ASC NULLS LAST]
          CudfUnloadExec
            CuDFSortExec: expr=[MinTemp@0 ASC NULLS LAST], preserve_partitioning=[true]
              CudfLoadExec
                DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp], file_type=parquet
        ");

        let cudf_results = plan.execute().await?;
        let host_results = tf.execute(host_sql).await?;
        assert_eq!(host_results.pretty_print, cudf_results.pretty_print);

        Ok(())
    }

    #[tokio::test]
    async fn test_sort_descending() -> Result<(), Box<dyn std::error::Error>> {
        let tf = TestFramework::new().await;

        let host_sql = r#"
            SELECT "MinTemp", "MaxTemp"
            FROM weather
            ORDER BY "MaxTemp" DESC
        "#;
        let cudf_sql = format!(r#" SET cudf.enable=true; {host_sql} "#);

        let plan = tf.plan(&cudf_sql).await?;
        assert_snapshot!(plan.display(), @r"
        SortPreservingMergeExec: [MaxTemp@1 DESC]
          CudfUnloadExec
            CuDFSortExec: expr=[MaxTemp@1 DESC], preserve_partitioning=[true]
              CudfLoadExec
                DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp], file_type=parquet
        ");

        let cudf_results = plan.execute().await?;
        let host_results = tf.execute(host_sql).await?;
        assert_eq!(host_results.pretty_print, cudf_results.pretty_print);

        Ok(())
    }

    #[tokio::test]
    async fn test_sort_with_limit() -> Result<(), Box<dyn std::error::Error>> {
        let tf = TestFramework::new().await;

        let host_sql = r#"
            SELECT "MinTemp", "MaxTemp"
            FROM weather
            ORDER BY "MinTemp" ASC
            LIMIT 3
        "#;
        let cudf_sql = format!(r#" SET cudf.enable=true; {host_sql} "#);

        let plan = tf.plan(&cudf_sql).await?;
        assert_snapshot!(plan.display(), @r"
        SortPreservingMergeExec: [MinTemp@0 ASC NULLS LAST], fetch=3
          CudfUnloadExec
            CuDFSortExec: TopK(fetch=3), expr=[MinTemp@0 ASC NULLS LAST], preserve_partitioning=[true]
              CudfLoadExec
                DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp], file_type=parquet, predicate=DynamicFilter [ empty ]
        ");

        let cudf_results = plan.execute().await?;
        let host_results = tf.execute(host_sql).await?;
        assert_eq!(host_results.pretty_print, cudf_results.pretty_print);

        Ok(())
    }
}
