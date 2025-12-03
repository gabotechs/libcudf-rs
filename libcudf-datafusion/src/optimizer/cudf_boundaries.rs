use crate::physical::{is_cudf_plan, CuDFLoadExec, CuDFUnloadExec};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::ExecutionPlan;
use std::sync::Arc;

#[derive(Debug)]
pub struct CuDFBoundariesRule;

impl PhysicalOptimizerRule for CuDFBoundariesRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let result = plan.transform_down(|parent| {
            let parent_is_cudf = is_cudf_plan(parent.as_ref());

            let children = parent.children();
            let mut new_children: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(children.len());
            let mut changed = false;
            for child in children {
                let child_is_cudf = is_cudf_plan(child.as_ref());

                if parent_is_cudf && !child_is_cudf && !parent.as_any().is::<CuDFLoadExec>() {
                    new_children.push(Arc::new(CuDFLoadExec::new(Arc::clone(child))));
                    changed = true;
                    continue;
                }

                if !parent_is_cudf && child_is_cudf && !child.as_any().is::<CuDFUnloadExec>() {
                    new_children.push(Arc::new(CuDFUnloadExec::new(Arc::clone(child))));
                    changed = true;
                    continue;
                }

                new_children.push(Arc::clone(child));
            }

            if changed {
                Ok(Transformed::yes(parent.with_new_children(new_children)?))
            } else {
                Ok(Transformed::no(parent))
            }
        })?;

        if is_cudf_plan(result.data.as_ref()) {
            Ok(Arc::new(CuDFUnloadExec::new(result.data)))
        } else {
            Ok(result.data)
        }
    }

    fn name(&self) -> &str {
        "CuDFBoundariesRule"
    }

    fn schema_check(&self) -> bool {
        false
    }
}
