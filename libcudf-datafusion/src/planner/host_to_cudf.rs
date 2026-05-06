use crate::aggregate::{try_as_cudf_aggregate, CuDFAggregateExec};
use crate::physical::{
    is_cudf_plan, try_as_cudf_hash_join, CuDFFilterExec, CuDFLoadExec, CuDFProjectionExec,
    CuDFSortExec, CuDFUnloadExec,
};
use crate::planner::CuDFConfig;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::aggregates::AggregateExec;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::HashJoinExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::ExecutionPlan;
use std::cell::Cell;
use std::sync::Arc;

fn try_as_cudf<T: ExecutionPlan + 'static>(
    r: datafusion::common::Result<T>,
) -> datafusion::common::Result<Option<Arc<dyn ExecutionPlan>>> {
    match r {
        Ok(n) => Ok(Some(Arc::new(n))),
        Err(DataFusionError::NotImplemented(_)) => Ok(None),
        Err(e) => Err(e),
    }
}

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
        let preserve_load_partitioning = cudf_config.cuda_streams;

        let result = plan.transform_up(|mut plan| {
            let mut cudf_node: Option<Arc<dyn ExecutionPlan>> = None;
            if let Some(node) = plan.as_any().downcast_ref::<FilterExec>() {
                cudf_node = try_as_cudf(CuDFFilterExec::try_new(node.clone()))?;
            }

            if let Some(node) = plan.as_any().downcast_ref::<ProjectionExec>() {
                cudf_node = try_as_cudf(CuDFProjectionExec::try_new(node.clone()))?;
            }

            if let Some(node) = plan.as_any().downcast_ref::<SortExec>() {
                cudf_node = Some(Arc::new(CuDFSortExec::try_new(node.clone())?));
            }

            if let Some(node) = plan.as_any().downcast_ref::<HashJoinExec>() {
                cudf_node = try_as_cudf_hash_join(node)?;
            }

            if let Some(node) = plan.as_any().downcast_ref::<AggregateExec>() {
                cudf_node = try_as_cudf_aggregate(node)?;
            }

            let mut changed = false;
            if let Some(node) = cudf_node {
                plan = node;
                changed = true;
            }

            let plan_is_cudf = is_cudf_plan(plan.as_ref());
            let children = plan.children();
            let mut new_children: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(children.len());
            for child in children.iter() {
                let child_is_cudf = is_cudf_plan(child.as_ref());

                if plan_is_cudf && !child_is_cudf && !plan.as_any().is::<CuDFLoadExec>() {
                    if let Some(cp) = child.as_any().downcast_ref::<CoalescePartitionsExec>() {
                        new_children.push(Arc::new(CuDFLoadExec::try_new_preserving_partitioning(
                            Arc::clone(cp.input()),
                            preserve_load_partitioning,
                        )?));
                    } else {
                        new_children.push(Arc::new(CuDFLoadExec::try_new_preserving_partitioning(
                            Arc::clone(child),
                            preserve_load_partitioning,
                        )?));
                    }
                    changed = true;
                    continue;
                }

                if !plan_is_cudf && child_is_cudf && !child.as_any().is::<CuDFUnloadExec>() {
                    let mut unload = CuDFUnloadExec::new(Arc::clone(child));
                    // Aggregations will expect a specific schema in, which is the one that was
                    // established while the node was placed there. As we are dealing with type
                    // incompatibilities in CuDF, we are tweaking the schema we return, and
                    // therefore, we might need to manually force a cast.
                    if let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() {
                        unload = unload.with_target_schema(Arc::clone(&agg.input_schema))
                    }
                    new_children.push(Arc::new(unload));
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

        let plan = if is_cudf_plan(result.data.as_ref()) {
            Arc::new(CuDFUnloadExec::new(result.data)) as Arc<dyn ExecutionPlan>
        } else {
            result.data
        };

        // After all CuDF nodes are inserted, assign a unique `segment_id` to
        // each contiguous GPU segment so stream-aware execution can key
        // streams by `(segment_id, partition)`.
        assign_segment_ids(plan)
    }

    fn name(&self) -> &str {
        "HostToCuDFRule"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

/// Walk the plan and assign a unique `segment_id` to each contiguous GPU
/// segment.
///
/// Segments start at every `CuDFLoadExec` (the bottom of a GPU pipeline) and
/// extend upward through CuDF nodes until interrupted by a non-CuDF parent.
/// Stream-eligible segments carry a nonzero id through every stream-aware
/// node. Components containing a non-stream-aware CuDF node keep segment id 0,
/// which means all operators use the default stream.
fn assign_segment_ids(
    plan: Arc<dyn ExecutionPlan>,
) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
    let counter = Cell::new(1usize);
    let (out, _segment) = walk_assign(plan, &counter, None)?;
    Ok(out)
}

fn is_stream_eligible_op(plan: &dyn ExecutionPlan) -> bool {
    let any = plan.as_any();
    any.is::<CuDFLoadExec>()
        || any.is::<CuDFFilterExec>()
        || any.is::<CuDFProjectionExec>()
        || any.is::<CuDFAggregateExec>()
        || any.is::<CuDFSortExec>()
        || any.is::<CuDFUnloadExec>()
}

fn cudf_component_only_allowlisted(plan: &dyn ExecutionPlan) -> bool {
    if !is_stream_eligible_op(plan) {
        return false;
    }

    plan.children().into_iter().all(|child| {
        !is_cudf_plan(child.as_ref()) || cudf_component_only_allowlisted(child.as_ref())
    })
}

/// Recursive helper: returns the rewritten plan and the segment id of its
/// outermost CuDF node (if any). The caller of a CuDF node uses the returned
/// id to tag itself.
fn walk_assign(
    plan: Arc<dyn ExecutionPlan>,
    counter: &Cell<usize>,
    inherited_stream_eligible: Option<bool>,
) -> datafusion::common::Result<(Arc<dyn ExecutionPlan>, Option<usize>)> {
    let plan_is_cudf = is_cudf_plan(plan.as_ref());
    let stream_eligible = if plan_is_cudf {
        inherited_stream_eligible.unwrap_or_else(|| cudf_component_only_allowlisted(plan.as_ref()))
    } else {
        false
    };

    // Recurse into children first.
    let original_children = plan.children();
    let mut new_children: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(original_children.len());
    let mut child_segments: Vec<Option<usize>> = Vec::with_capacity(original_children.len());
    for child in &original_children {
        let child_stream_eligible = if plan_is_cudf && is_cudf_plan(child.as_ref()) {
            Some(stream_eligible)
        } else {
            None
        };
        let (new_child, seg) = walk_assign(Arc::clone(child), counter, child_stream_eligible)?;
        new_children.push(new_child);
        child_segments.push(seg);
    }

    // Determine this node's segment id.
    let any = plan.as_any();
    let this_segment = if any.is::<CuDFLoadExec>() {
        if stream_eligible {
            // A new stream-eligible segment starts here.
            let id = counter.get();
            counter.set(id + 1);
            Some(id)
        } else {
            Some(0)
        }
    } else if plan_is_cudf {
        // Inherit from CuDF child (single-segment plans converge to one id).
        // Pick the first child segment id, which should match all CuDF children.
        Some(if stream_eligible {
            child_segments.iter().filter_map(|s| *s).next().unwrap_or(0)
        } else {
            0
        })
    } else {
        None
    };

    // Rebuild the node with new children, then tag it with segment id if it's
    // a CuDF node we care about.
    let mut rebuilt = if !original_children.is_empty()
        && original_children
            .iter()
            .zip(new_children.iter())
            .any(|(a, b)| !Arc::ptr_eq(a, b))
    {
        plan.clone().with_new_children(new_children)?
    } else {
        plan
    };

    if let Some(seg) = this_segment {
        if let Some(load) = rebuilt.as_any().downcast_ref::<CuDFLoadExec>() {
            rebuilt = Arc::new(load.with_segment_id(seg));
        } else if let Some(filter) = rebuilt.as_any().downcast_ref::<CuDFFilterExec>() {
            rebuilt = Arc::new(filter.with_segment_id(seg));
        } else if let Some(projection) = rebuilt.as_any().downcast_ref::<CuDFProjectionExec>() {
            rebuilt = Arc::new(projection.with_segment_id(seg));
        } else if let Some(agg) = rebuilt.as_any().downcast_ref::<CuDFAggregateExec>() {
            rebuilt = Arc::new(agg.with_segment_id(seg));
        } else if let Some(sort) = rebuilt.as_any().downcast_ref::<CuDFSortExec>() {
            rebuilt = Arc::new(sort.with_segment_id(seg));
        } else if let Some(unload) = rebuilt.as_any().downcast_ref::<CuDFUnloadExec>() {
            rebuilt = Arc::new(unload.with_segment_id(seg));
        }
    }

    Ok((rebuilt, this_segment))
}
