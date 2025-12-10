use crate::optimizer::CuDFConfig;
use crate::physical::{
    is_cudf_plan, CuDFCoalesceBatchesExec, CuDFFilterExec, CuDFProjectionExec, CuDFSortExec,
};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::projection::ProjectionExec;
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

        let result = plan.transform_up(|plan| {
            if let Some(node) = plan.as_any().downcast_ref::<FilterExec>() {
                return Ok(Transformed::yes(Arc::new(CuDFFilterExec::try_new(
                    node.clone(),
                )?)));
            }

            if let Some(node) = plan.as_any().downcast_ref::<ProjectionExec>() {
                return Ok(Transformed::yes(Arc::new(CuDFProjectionExec::try_new(
                    node.clone(),
                )?)));
            }

            if let Some(node) = plan.as_any().downcast_ref::<SortExec>() {
                return Ok(Transformed::yes(Arc::new(CuDFSortExec::try_new(
                    node.clone(),
                )?)));
            }

            if let Some(node) = plan.as_any().downcast_ref::<CoalesceBatchesExec>() {
                if is_cudf_plan(node.input().as_ref()) {
                    return Ok(Transformed::yes(Arc::new(CuDFCoalesceBatchesExec::new(
                        node.clone(),
                    ))));
                }
            }

            Ok(Transformed::no(plan))
        })?;

        Ok(result.data)
    }

    fn name(&self) -> &str {
        "HostToCuDFRule"
    }

    fn schema_check(&self) -> bool {
        false
    }
}
