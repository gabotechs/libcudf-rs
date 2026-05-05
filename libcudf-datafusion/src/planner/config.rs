use datafusion::common::{extensions_options, plan_err, DataFusionError};
use datafusion::config::{ConfigExtension, ConfigOptions};

extensions_options! {
    pub struct CuDFConfig {
        /// Enables CuDF optimizations.
        pub enable: bool, default = true
        /// Batch size for moving data from CPU to GPU and vice-versa.
        pub batch_size: usize, default = 8192 * 10
        /// Allocate record batches using pinned (page-locked) memory via `cudaMallocHost`
        /// instead of the default allocator for arrow arrays. Pinned-source `cudaMemcpyAsync`
        /// is fully asynchronous, allowing us to do H -> D copies much faster.
        pub pinned_input: bool, default = true
    }
}

impl ConfigExtension for CuDFConfig {
    const PREFIX: &'static str = "cudf";
}

impl CuDFConfig {
    /// Gets the [CuDFConfig] from the [ConfigOptions]'s extensions.
    pub fn from_config_options(cfg: &ConfigOptions) -> Result<&Self, DataFusionError> {
        let Some(distributed_cfg) = cfg.extensions.get::<CuDFConfig>() else {
            return plan_err!("CuDFConfig is not in ConfigOptions.extensions");
        };
        Ok(distributed_cfg)
    }

    /// Gets the [CuDFConfig] from the [ConfigOptions]'s extensions.
    pub fn from_config_options_mut(cfg: &mut ConfigOptions) -> Result<&mut Self, DataFusionError> {
        let Some(distributed_cfg) = cfg.extensions.get_mut::<CuDFConfig>() else {
            return plan_err!("CuDFConfig is not in ConfigOptions.extensions");
        };
        Ok(distributed_cfg)
    }
}
