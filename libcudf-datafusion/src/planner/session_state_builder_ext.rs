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

        let target_partitions = cfg.options_mut().execution.target_partitions;
        // Assume only one GPU present, and therefore, force target_partitions == 1.
        cfg.options_mut().execution.target_partitions = 1;

        self.with_physical_optimizer_rule(Arc::new(RescaleLeafsRule(target_partitions)))
            .with_physical_optimizer_rule(Arc::new(HostToCuDFRule))
    }
}
