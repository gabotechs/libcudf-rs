use crate::physical::{
    CuDFAggregateExec, CuDFCoalescePartitionsExec, CuDFFilterExec, CuDFParquetScanExec,
    CuDFProjectionExec,
};
use crate::planner::CuDFConfig;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::aggregates::AggregateMode;
use datafusion_physical_plan::{ChildrenPropertiesMode, ExecutionPlan, ReplaceChildrenOptions};
use std::sync::Arc;

/// Moves the single-partition boundary above a partial aggregate.
///
/// Before:
/// ```text
/// CuDFUnloadExec                              [1]
///   CuDFProjectionExec                        [1]
///     CuDFSortExec                            [1]
///       CuDFAggregateExec: mode=Single        [1]
///         CuDFProjectionExec                  [1]
///           CuDFFilterExec                    [1]
///             CuDFCoalescePartitionsExec      [8 -> 1]
///               CuDFParquetScanExec           [8]
/// ```
///
/// The rewrite uses one CUDA stream per partition so partial aggregations can
/// run concurrently. The coalescer produces one DataFusion partition while
/// preserving the batches' streams; the final aggregate reconciles those
/// streams and emits one batch:
/// ```text
/// CuDFUnloadExec                              [1]
///   CuDFProjectionExec                        [1]
///     CuDFSortExec                            [1]
///       CuDFAggregateExec: mode=Final         [1]
///         CuDFCoalescePartitionsExec          [8 -> 1]
///           CuDFAggregateExec: mode=Partial   [8]
///             CuDFProjectionExec              [8]
///               CuDFFilterExec                [8]
///                 CuDFParquetScanExec         [8]
/// ```
#[derive(Debug)]
pub(crate) struct ParquetAggregateStreamsRule;

impl PhysicalOptimizerRule for ParquetAggregateStreamsRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let partitions = CuDFConfig::try_get(config)?.parquet_scan_streams;
        if partitions == 1 {
            return Ok(plan);
        }

        plan.transform_up(|plan| split_aggregate(plan, partitions))
            .map(|plan| plan.data)
    }

    fn name(&self) -> &str {
        "ParquetAggregateStreamsRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Plan nodes that are allowed between the aggregate and parquet scan.
static PARTITION_NATIVE_EXEC_TYPES: &[fn(&dyn ExecutionPlan) -> bool] = &[
    |plan| plan.is::<CuDFProjectionExec>(),
    |plan| plan.is::<CuDFFilterExec>(),
];

fn split_aggregate(
    plan: Arc<dyn ExecutionPlan>,
    partitions: usize,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(aggregate) = plan.downcast_ref::<CuDFAggregateExec>() else {
        return Ok(Transformed::no(plan));
    };
    if aggregate.mode() != AggregateMode::Single {
        return Ok(Transformed::no(plan));
    }
    let Some(input) = repartition_pipeline(Arc::clone(aggregate.input()), partitions)? else {
        return Ok(Transformed::no(plan));
    };

    let group_by = aggregate.group_by().clone();
    let aggr_expr = aggregate.aggr_expr();
    let partial: Arc<dyn ExecutionPlan> = Arc::new(CuDFAggregateExec::try_new(
        input,
        AggregateMode::Partial,
        group_by.clone(),
        aggr_expr.clone(),
    )?);
    let coalesced: Arc<dyn ExecutionPlan> = Arc::new(CuDFCoalescePartitionsExec::new(partial));
    let final_aggregate = CuDFAggregateExec::try_new(
        coalesced,
        AggregateMode::Final,
        group_by.as_final(),
        aggr_expr,
    )?;
    Ok(Transformed::yes(Arc::new(final_aggregate)))
}

fn repartition_pipeline(
    plan: Arc<dyn ExecutionPlan>,
    partitions: usize,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if let Some(scan) = plan.downcast_ref::<CuDFParquetScanExec>() {
        return scan.repartitioned_for_cuda_streams(partitions);
    }

    if PARTITION_NATIVE_EXEC_TYPES
        .iter()
        .any(|is_type| is_type(plan.as_ref()))
    {
        let Some(input) = repartition_pipeline(Arc::clone(plan.children()[0]), partitions)? else {
            return Ok(None);
        };
        return replace_children(plan, vec![input]).map(Some);
    }

    if plan.is::<CuDFCoalescePartitionsExec>() && plan.fetch().is_none() {
        return repartition_pipeline(Arc::clone(plan.children()[0]), partitions);
    }

    Ok(None)
}

fn replace_children(
    plan: Arc<dyn ExecutionPlan>,
    children: Vec<Arc<dyn ExecutionPlan>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    plan.replace_children(
        children,
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
    )
}

#[cfg(test)]
mod tests {
    use crate::assert_snapshot;
    use crate::test_utils::TestFramework;

    #[tokio::test]
    async fn partitions_parquet_aggregate_pipeline() -> Result<(), Box<dyn std::error::Error>> {
        let tf = TestFramework::new().await;
        let query = r#"
            SELECT "RainToday",
                   SUM("MinTemp" + "MaxTemp"),
                   AVG("MinTemp" + "MaxTemp")
            FROM weather
            WHERE "Rainfall" > 0
            GROUP BY "RainToday"
        "#;
        let before_sql = format!(
            r#"
                SET datafusion.optimizer.repartition_file_min_size=0;
                SET cudf.parquet_scan=true;
                SET cudf.parquet_scan_streams=1;
                {query}
            "#
        );
        let before = tf.plan(&before_sql).await?;
        assert_snapshot!(before.display(), @r"
        CuDFUnloadExec
          CuDFAggregateExec: mode=Single, group_by=[RainToday@RainToday@1], aggr_expr=[sum(weather.MinTemp + weather.MaxTemp), avg(weather.MinTemp + weather.MaxTemp)]
            CuDFProjectionExec: expr=[MinTemp@0 + MaxTemp@1 as __common_expr_1, RainToday@2 as RainToday]
              CuDFFilterExec: Rainfall@2 > 0, projection=[MinTemp@0, MaxTemp@1, RainToday@3]
                CuDFCoalescePartitionsExec
                  CuDFParquetScanExec: files=3, batches=3, files_per_batch=3, chunk_read_limit=268435456, pass_read_limit=268435456, read_columns=4, filter=false
        ");

        let after_sql = format!(
            r#"
                SET datafusion.optimizer.repartition_file_min_size=0;
                SET cudf.parquet_scan_streams=4;
                {query}
            "#
        );
        let after = tf.plan(&after_sql).await?;
        assert_snapshot!(after.display(), @r"
        CuDFUnloadExec
          CuDFAggregateExec: mode=Final, group_by=[RainToday@RainToday@0], aggr_expr=[sum(weather.MinTemp + weather.MaxTemp), avg(weather.MinTemp + weather.MaxTemp)]
            CuDFCoalescePartitionsExec
              CuDFAggregateExec: mode=Partial, group_by=[RainToday@RainToday@1], aggr_expr=[sum(weather.MinTemp + weather.MaxTemp), avg(weather.MinTemp + weather.MaxTemp)]
                CuDFProjectionExec: expr=[MinTemp@0 + MaxTemp@1 as __common_expr_1, RainToday@2 as RainToday]
                  CuDFFilterExec: Rainfall@2 > 0, projection=[MinTemp@0, MaxTemp@1, RainToday@3]
                    CuDFParquetScanExec: files=3, batches=3, files_per_batch=3, chunk_read_limit=268435456, pass_read_limit=268435456, read_columns=4, filter=false, cuda_streams=true
        ");
        Ok(())
    }
}
