use crate::planner::host_to_cudf::HostToCuDFRule;
use crate::planner::rescale_leafs::RescaleLeafsRule;
use crate::CuDFConfig;
use datafusion::execution::SessionStateBuilder;
use std::sync::Arc;

/// Extension trait for [SessionStateBuilder].
pub trait SessionStateBuilderExt {
    /// Installs the cuDF physical optimizer rules.
    fn with_cudf_planner(self) -> Self;
}

impl SessionStateBuilderExt for SessionStateBuilder {
    fn with_cudf_planner(mut self) -> Self {
        let cfg = self.config().get_or_insert_default();
        if cfg.options().extensions.get::<CuDFConfig>().is_none() {
            cfg.options_mut().extensions.insert(CuDFConfig::default());
        }

        let cuda_streams = cfg
            .options()
            .extensions
            .get::<CuDFConfig>()
            .is_some_and(|c| c.cuda_streams);

        let target_partitions = cfg.options_mut().execution.target_partitions;
        // Without streams, all GPU work serializes on the default stream,
        // so multiple partitions add scheduler overhead with no parallelism.
        // Force `target_partitions == 1` for the GPU subplan and let the
        // RescaleLeafsRule keep the CPU-side leafs at their original count.
        //
        // With streams, each partition gets its own CUDA stream and they can
        // run in parallel on the GPU's copy engines / SMs, so leave
        // `target_partitions` unchanged.
        if !cuda_streams {
            cfg.options_mut().execution.target_partitions = 1;
        }

        self.with_physical_optimizer_rule(Arc::new(RescaleLeafsRule(target_partitions)))
            .with_physical_optimizer_rule(Arc::new(HostToCuDFRule))
    }
}
