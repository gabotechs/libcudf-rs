use crate::errors::cudf_to_df;
use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::common::Statistics;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::LexOrdering;
use datafusion_physical_plan::execution_plan::CardinalityEffect;
use datafusion_physical_plan::expressions::Column;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    execute_stream, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use delegate::delegate;
use futures::Stream;
use futures_util::{ready, StreamExt};
use libcudf_rs::{
    gather, slice_column, sort, stable_sorted_order, CuDFTable, CuDFTableView, SortOrder,
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

        let schema = self.schema();
        let ordered_stream = if let Some(limit) = self.fetch() {
            CuDFTopKStream {
                input,
                schema,
                limit,
                ordering: self.inner.expr().clone(),
                result: None,
                finished: false,
            }
            .left_stream()
        } else {
            CuDFSortStream {
                input,
                schema,
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
            fn properties(&self) -> &Arc<PlanProperties>;
            fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>>;
            fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics, DataFusionError>;
            fn fetch(&self) -> Option<usize>;
            fn cardinality_effect(&self) -> CardinalityEffect;
        }
    }
}

struct CuDFSortStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
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

                let (key_columns, sort_orders) = extract_sort_params(&self.ordering);
                let sorted = sort(&table_view, &key_columns, &sort_orders).map_err(cudf_to_df)?;

                let result = sorted
                    .into_view()
                    .to_record_batch_with_schema(&self.schema)
                    .map_err(cudf_to_df)?;

                Poll::Ready(Some(Ok(result)))
            }
        }
    }
}

/// Stream that efficiently computes Top-K by maintaining only K elements
///
/// Instead of accumulating all input data and sorting at the end, this stream
/// keeps only the top K rows at each step. When a new batch arrives, it:
/// 1. Concatenates with existing top K rows
/// 2. If total > K, sorts and keeps only top K
/// 3. Otherwise, keeps all rows (will sort at the end)
///
/// This reduces memory usage and improves performance for large inputs.
struct CuDFTopKStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
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
                let new_table = CuDFTableView::from_record_batch(&batch).map_err(cudf_to_df)?;

                let merged_table = if let Some(existing) = self.result.take() {
                    let views = vec![existing, new_table];
                    CuDFTable::concat(views).map_err(cudf_to_df)?.into_view()
                } else {
                    new_table
                };

                // Keep only top K rows to avoid accumulating all data
                if merged_table.num_rows() > self.limit {
                    let (key_columns, sort_orders) = extract_sort_params(&self.ordering);
                    let key_views = key_columns
                        .iter()
                        .map(|&i| merged_table.column(i as i32))
                        .collect();
                    let keys_view =
                        CuDFTableView::from_column_views(key_views).map_err(cudf_to_df)?;

                    // Get sorted indices
                    let indices =
                        stable_sorted_order(&keys_view, &sort_orders).map_err(cudf_to_df)?;

                    // Slice to keep only top K indices
                    let indices_view = Arc::new(indices).view();
                    let topk_indices_view =
                        slice_column(&indices_view, 0, self.limit).map_err(cudf_to_df)?;

                    // Gather the top K rows
                    let topk_table =
                        gather(&merged_table, &topk_indices_view).map_err(cudf_to_df)?;
                    self.result = Some(topk_table.into_view());
                } else {
                    self.result = Some(merged_table);
                }

                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Some(Err(err)) => Poll::Ready(Some(Err(err))),
            None => {
                self.finished = true;
                let Some(result) = self.result.take() else {
                    return Poll::Ready(None);
                };

                // At this point we have <= K rows accumulated from all batches
                // Just sort them to get the final top K result
                let (key_columns, sort_orders) = extract_sort_params(&self.ordering);
                let sorted_result =
                    sort(&result, &key_columns, &sort_orders).map_err(cudf_to_df)?;

                let batch = sorted_result
                    .into_view()
                    .to_record_batch_with_schema(&self.schema)
                    .map_err(cudf_to_df)?;
                Poll::Ready(Some(Ok(batch)))
            }
        }
    }
}

fn extract_sort_params(ordering: &LexOrdering) -> (Vec<usize>, Vec<SortOrder>) {
    ordering
        .iter()
        .map(|expr| {
            let col_idx = expr
                .expr
                .as_any()
                .downcast_ref::<Column>()
                .map(|c| c.index())
                .unwrap_or(0);
            let sort_order = match (expr.options.descending, expr.options.nulls_first) {
                (false, true) => SortOrder::AscendingNullsFirst,
                (false, false) => SortOrder::AscendingNullsLast,
                (true, true) => SortOrder::DescendingNullsFirst,
                (true, false) => SortOrder::DescendingNullsLast,
            };
            (col_idx, sort_order)
        })
        .unzip()
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
          CuDFUnloadExec
            CuDFSortExec: expr=[MinTemp@0 ASC NULLS LAST], preserve_partitioning=[true]
              CuDFLoadExec
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
          CuDFUnloadExec
            CuDFSortExec: expr=[MaxTemp@1 DESC], preserve_partitioning=[true]
              CuDFLoadExec
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
          CuDFUnloadExec
            CuDFSortExec: TopK(fetch=3), expr=[MinTemp@0 ASC NULLS LAST], preserve_partitioning=[true]
              CuDFLoadExec
                DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp], file_type=parquet, predicate=DynamicFilter [ empty ]
        ");

        let cudf_results = plan.execute().await?;
        let host_results = tf.execute(host_sql).await?;
        assert_eq!(host_results.pretty_print, cudf_results.pretty_print);

        Ok(())
    }
}
