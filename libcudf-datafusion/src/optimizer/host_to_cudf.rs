use crate::physical::CuDFFilterExec;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::ExecutionPlan;
use std::sync::Arc;

#[derive(Debug)]
pub struct HostToCuDFRule;

impl PhysicalOptimizerRule for HostToCuDFRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let result = plan.transform_down(|plan| {
            if let Some(filter) = plan.as_any().downcast_ref::<FilterExec>() {
                return Ok(Transformed::yes(Arc::new(CuDFFilterExec::new(
                    filter.clone(),
                ))));
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
