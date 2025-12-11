use crate::optimizer::CuDFConfig;
use crate::physical::{
    is_cudf_plan, CuDFCoalesceBatchesExec, CuDFFilterExec, CuDFLoadExec, CuDFProjectionExec,
    CuDFSortExec, CuDFUnloadExec,
};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::ExecutionPlan;
use std::sync::Arc;

#[derive(Debug)]
pub struct HostToCuDFRule;

impl PhysicalOptimizerRule for HostToCuDFRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let Some(cudf_config) = config.extensions.get::<CuDFConfig>() else {
            return Ok(plan);
        };
        if !cudf_config.enable {
            return Ok(plan);
        }

        let result = plan.transform_up(|mut plan| {
            let mut cudf_node: Option<Arc<dyn ExecutionPlan>> = None;
            if let Some(node) = plan.as_any().downcast_ref::<FilterExec>() {
                cudf_node = Some(Arc::new(CuDFFilterExec::try_new(node.clone())?));
            }

            if let Some(node) = plan.as_any().downcast_ref::<ProjectionExec>() {
                cudf_node = Some(Arc::new(CuDFProjectionExec::try_new(node.clone())?));
            }

            if let Some(node) = plan.as_any().downcast_ref::<SortExec>() {
                cudf_node = Some(Arc::new(CuDFSortExec::try_new(node.clone())?));
            }

            if let Some(node) = plan.as_any().downcast_ref::<CoalesceBatchesExec>() {
                if is_cudf_plan(node.input().as_ref()) {
                    cudf_node = Some(Arc::new(CuDFCoalesceBatchesExec::new(node.clone())));
                }
            }

            let mut changed = false;
            if let Some(node) = cudf_node {
                plan = node;
                changed = true;
            }

            let plan_is_cudf = is_cudf_plan(plan.as_ref());
            let children = plan.children();
            let mut new_children: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(children.len());
            for mut child in plan.children() {
                let child_is_cudf = is_cudf_plan(child.as_ref());

                if plan_is_cudf && !child_is_cudf && !plan.as_any().is::<CuDFLoadExec>() {
                    // CuDFLoadExec coalesces everything into one partition (assuming one GPU), so
                    // having a repartition is pointless, as we will just join all the output
                    // partitions into one.
                    if child.as_any().is::<RepartitionExec>() {
                        child = child.children().swap_remove(0)
                    }
                    new_children.push(Arc::new(CuDFLoadExec::try_new(Arc::clone(child))?));
                    changed = true;
                    continue;
                }

                if !plan_is_cudf && child_is_cudf && !child.as_any().is::<CuDFUnloadExec>() {
                    new_children.push(Arc::new(CuDFUnloadExec::new(Arc::clone(child))));
                    changed = true;
                    continue;
                }

                new_children.push(Arc::clone(child));
            }

            if changed {
                Ok(Transformed::yes(plan.with_new_children(new_children)?))
            } else {
                Ok(Transformed::no(plan))
            }
        })?;

        if is_cudf_plan(result.data.as_ref()) {
            Ok(Arc::new(CuDFUnloadExec::new(result.data)))
        } else {
            Ok(result.data)
        }
    }

    fn name(&self) -> &str {
        "HostToCuDFRule"
    }

    fn schema_check(&self) -> bool {
        false
    }
}
