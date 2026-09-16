use crate::physical::{
    CuDFAggregateExec, CuDFCoalescePartitionsExec, CuDFFilterExec, CuDFParquetScanExec,
    CuDFProjectionExec, CuDFSortExec, CuDFUnloadExec,
};
use crate::planner::CuDFConfig;
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::{
    ChildrenPropertiesMode, ExecutionPlan, ExecutionPlanProperties, ReplaceChildrenOptions,
};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct ParquetAggregateStreamsRule;

impl PhysicalOptimizerRule for ParquetAggregateStreamsRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let stream_count = CuDFConfig::try_get(config)?.parquet_scan_streams;
        if stream_count == 1 {
            return Ok(plan);
        }

        rewrite_plan(plan, false, false, stream_count).map(|(plan, _)| plan)
    }

    fn name(&self) -> &str {
        "ParquetAggregateStreamsRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn rewrite_plan(
    plan: Arc<dyn ExecutionPlan>,
    output_stream_safe: bool,
    below_aggregate: bool,
    stream_count: usize,
) -> datafusion::common::Result<(Arc<dyn ExecutionPlan>, bool)> {
    if let Some(scan) = plan.downcast_ref::<CuDFParquetScanExec>() {
        return if below_aggregate {
            Ok((Arc::new(scan.with_stream_count(stream_count)?), true))
        } else {
            Ok((plan, false))
        };
    }

    let (child_stream_safe, child_below_aggregate) = if plan.is::<CuDFUnloadExec>() {
        (true, false)
    } else if plan.is::<CuDFAggregateExec>() {
        (output_stream_safe, output_stream_safe)
    } else if plan.is::<CuDFProjectionExec>() || plan.is::<CuDFFilterExec>() {
        (output_stream_safe, below_aggregate)
    } else if plan.is::<CuDFCoalescePartitionsExec>() {
        let one_partition = plan.children()[0].output_partitioning().partition_count() == 1;
        (output_stream_safe, below_aggregate && one_partition)
    } else if plan.is::<CuDFSortExec>() {
        (output_stream_safe, false)
    } else {
        (false, false)
    };

    let mut changed = false;
    let mut children = Vec::with_capacity(plan.children().len());
    for child in plan.children() {
        let (child, child_changed) = rewrite_plan(
            Arc::clone(child),
            child_stream_safe,
            child_below_aggregate,
            stream_count,
        )?;
        changed |= child_changed;
        children.push(child);
    }
    if changed {
        Ok((replace_children(plan, children)?, true))
    } else {
        Ok((plan, false))
    }
}

fn replace_children(
    plan: Arc<dyn ExecutionPlan>,
    children: Vec<Arc<dyn ExecutionPlan>>,
) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
    plan.replace_children(
        children,
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::TestFramework;

    #[tokio::test]
    async fn enables_streams_only_below_aggregates() -> Result<(), Box<dyn std::error::Error>> {
        let tf = TestFramework::new().await;
        let aggregate = tf
            .plan(
                "SET cudf.parquet_scan=true; \
                 SET cudf.parquet_scan_streams=4; \
                 SELECT \"RainToday\", SUM(\"Rainfall\") FROM weather GROUP BY \"RainToday\"",
            )
            .await?;
        let projection = tf
            .plan(
                "SELECT \"MinTemp\" + \"MaxTemp\" FROM weather \
                 WHERE \"Rainfall\" > 0",
            )
            .await?;
        let join = tf
            .plan(
                "SELECT grouped.\"MinTemp\" \
                 FROM (SELECT \"MinTemp\", SUM(\"Rainfall\") \
                       FROM weather GROUP BY \"MinTemp\") grouped \
                 JOIN weather raw ON grouped.\"MinTemp\" = raw.\"MinTemp\"",
            )
            .await?;

        assert_eq!(scan_stream_counts(&aggregate.plan), vec![4]);
        assert_eq!(
            scan_stream_counts(&projection.plan),
            vec![1],
            "{}",
            projection.display()
        );
        assert_eq!(scan_stream_counts(&join.plan), vec![1, 1]);
        Ok(())
    }

    fn scan_stream_counts(plan: &Arc<dyn ExecutionPlan>) -> Vec<usize> {
        let mut counts = plan
            .downcast_ref::<CuDFParquetScanExec>()
            .map(|scan| vec![scan.stream_count()])
            .unwrap_or_default();
        for child in plan.children() {
            counts.extend(scan_stream_counts(child));
        }
        counts
    }
}
