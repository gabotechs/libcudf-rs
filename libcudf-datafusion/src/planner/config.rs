use datafusion::common::{extensions_options, plan_err, DataFusionError};
use datafusion::config::{ConfigExtension, ConfigOptions};
use libcudf_rs::DevicePoolConfig;

const AGGREGATE_CHUNK_POOL_FRACTION: usize = 16;
const MIN_AGGREGATE_CHUNK_TARGET_BYTES: usize = 64 * 1024 * 1024;
const MAX_AGGREGATE_CHUNK_TARGET_BYTES: usize = 1024 * 1024 * 1024;

extensions_options! {
    pub struct CuDFConfig {
        /// Enables CuDF optimizations.
        pub enable: bool, default = true
        /// Allocate record batches using pinned (page-locked) memory via `cudaMallocHost`
        /// instead of the default allocator for arrow arrays. Pinned-source `cudaMemcpyAsync`
        /// is fully asynchronous, allowing us to do H -> D copies much faster.
        pub pinned_input: bool, default = true
        /// Configured RMM device-memory pool cap used to derive operator memory budgets.
        pub device_pool_max_bytes: Option<usize>, default = None
        /// Target input bytes accumulated by each cuDF aggregate chunk before flushing.
        pub aggregate_chunk_target_bytes: Option<usize>, default = None
    }
}

impl ConfigExtension for CuDFConfig {
    const PREFIX: &'static str = "cudf";
}

impl CuDFConfig {
    /// Derive the default cuDF aggregate input chunk budget from a device-pool cap.
    pub fn derive_aggregate_chunk_target_bytes(device_pool_max_bytes: usize) -> usize {
        (device_pool_max_bytes / AGGREGATE_CHUNK_POOL_FRACTION).clamp(
            MIN_AGGREGATE_CHUNK_TARGET_BYTES,
            MAX_AGGREGATE_CHUNK_TARGET_BYTES,
        )
    }

    /// Resolve the configured device-pool cap, falling back to the default pool config.
    pub fn resolved_device_pool_max_bytes(&self) -> usize {
        self.device_pool_max_bytes
            .unwrap_or_else(|| DevicePoolConfig::default().max_size)
    }

    /// Resolve the cuDF aggregate input chunk budget.
    pub fn resolved_aggregate_chunk_target_bytes(&self) -> usize {
        self.aggregate_chunk_target_bytes.unwrap_or_else(|| {
            Self::derive_aggregate_chunk_target_bytes(self.resolved_device_pool_max_bytes())
        })
    }

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

#[cfg(test)]
mod tests {
    use super::CuDFConfig;

    #[test]
    fn aggregate_chunk_target_resolves_from_override_or_pool_cap() {
        assert_eq!(
            CuDFConfig::default().resolved_aggregate_chunk_target_bytes(),
            256 * 1024 * 1024
        );
        assert_eq!(
            CuDFConfig::derive_aggregate_chunk_target_bytes(4 * 1024 * 1024 * 1024),
            256 * 1024 * 1024
        );
        assert_eq!(
            CuDFConfig {
                device_pool_max_bytes: Some(12 * 1024 * 1024 * 1024),
                ..Default::default()
            }
            .resolved_aggregate_chunk_target_bytes(),
            768 * 1024 * 1024,
        );
        assert_eq!(
            CuDFConfig::derive_aggregate_chunk_target_bytes(128 * 1024 * 1024 * 1024),
            1024 * 1024 * 1024
        );
        assert_eq!(
            CuDFConfig {
                device_pool_max_bytes: Some(12 * 1024 * 1024 * 1024),
                aggregate_chunk_target_bytes: Some(1234),
                ..Default::default()
            }
            .resolved_aggregate_chunk_target_bytes(),
            1234,
        );
    }
}
