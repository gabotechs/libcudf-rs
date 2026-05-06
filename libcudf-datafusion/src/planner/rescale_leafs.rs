use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Fan each leaf node out to [`CuDFConfig::leaf_node_partitions`] partitions
/// when the rest of the plan is running on a single partition
/// (`datafusion.execution.target_partitions == 1`). This parallelizes the
/// I/O fringe without affecting the GPU pipeline above.
#[derive(Debug)]
pub(crate) struct RescaleLeafsRule(pub(crate) usize);

impl PhysicalOptimizerRule for RescaleLeafsRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let Self(leaf_node_partitions) = self;

        let transformed = plan.transform_up(|plan| {
            if !plan.children().is_empty() {
                return Ok(Transformed::no(plan));
            }

            match plan.repartitioned(*leaf_node_partitions, config)? {
                Some(rescaled) => Ok(Transformed::yes(Arc::new(CoalescePartitionsExec::new(
                    rescaled,
                )))),
                None => Ok(Transformed::no(plan)),
            }
        })?;
        Ok(transformed.data)
    }

    fn name(&self) -> &str {
        "RescaleLeafsRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
