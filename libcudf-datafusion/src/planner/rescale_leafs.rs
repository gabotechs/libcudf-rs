use crate::physical::{CuDFCoalescePartitionsExec, CuDFLoadExec, CuDFParquetScanExec};
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::{ChildrenPropertiesMode, ExecutionPlan, ReplaceChildrenOptions};
use std::sync::Arc;

/// Repartition scan leaves to `target_partitions` while keeping the rest
/// of the plan single-partitioned.
#[derive(Debug)]
pub(crate) struct RescaleLeafsRule(pub(crate) usize);

impl PhysicalOptimizerRule for RescaleLeafsRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Self(leaf_node_partitions) = self;
        if *leaf_node_partitions == 1 {
            return Ok(plan);
        }
        rescale_scans(plan, *leaf_node_partitions, config)
    }

    fn name(&self) -> &str {
        "RescaleLeafsRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn rescale_scans(
    plan: Arc<dyn ExecutionPlan>,
    target_partitions: usize,
    config: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    if plan.is::<CuDFLoadExec>() && plan.children()[0].is::<DataSourceExec>() {
        let input = plan.children()[0];
        let Some(input) = input.repartitioned(target_partitions, config)? else {
            return Ok(plan);
        };
        return replace_children(plan, vec![input]);
    }

    if let Some(scan) = plan.downcast_ref::<CuDFParquetScanExec>() {
        if scan.cuda_streams_enabled() {
            return Ok(plan);
        }
        return rescale_scan(plan, target_partitions, config, true);
    }

    if plan.is::<DataSourceExec>() {
        return rescale_scan(plan, target_partitions, config, false);
    }

    let children = plan.children();
    let mut changed = false;
    let mut new_children = Vec::with_capacity(children.len());
    for child in children {
        let child = Arc::clone(child);
        let new_child = rescale_scans(Arc::clone(&child), target_partitions, config)?;
        changed |= !Arc::ptr_eq(&child, &new_child);
        new_children.push(new_child);
    }

    if changed {
        replace_children(plan, new_children)
    } else {
        Ok(plan)
    }
}

fn rescale_scan(
    plan: Arc<dyn ExecutionPlan>,
    target_partitions: usize,
    config: &ConfigOptions,
    cudf: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    let Some(rescaled) = plan.repartitioned(target_partitions, config)? else {
        return Ok(plan);
    };
    if rescaled
        .properties()
        .output_partitioning()
        .partition_count()
        == 1
    {
        return Ok(rescaled);
    }

    if cudf {
        Ok(Arc::new(CuDFCoalescePartitionsExec::new(rescaled)))
    } else {
        Ok(Arc::new(CoalescePartitionsExec::new(rescaled)))
    }
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
