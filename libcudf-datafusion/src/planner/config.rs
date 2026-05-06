use crate::stream_source::CuDFStreamSource;
use datafusion::common::{extensions_options, plan_err, DataFusionError};
use datafusion::config::{ConfigExtension, ConfigOptions};

extensions_options! {
    pub struct CuDFConfig {
        /// Enables CuDF optimizations.
        pub enable: bool, default = true
        /// Target input bytes accumulated by each cuDF aggregate chunk before flushing.
        pub aggregate_chunk_target_bytes: usize, default = 256 * 1024 * 1024
        /// Allocate record batches using pinned (page-locked) memory via `cudaMallocHost`
        /// instead of the default allocator for arrow arrays. Pinned-source `cudaMemcpyAsync`
        /// is fully asynchronous, allowing us to do H -> D copies much faster.
        pub pinned_input: bool, default = true
        /// Use a CUDA stream per (segment, partition) to overlap independent GPU work
        /// across partitions. When `false`, all cuDF work runs on the default stream.
        pub cuda_streams: bool, default = false
        /// Allocates CUDA streams for query execution. This is private runtime wiring,
        /// not a user-facing config surface.
        pub(crate) __private_stream_source: CuDFStreamSource, default = CuDFStreamSource::default()
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

    /// Source for allocating CUDA streams for stream-aware execution.
    pub(crate) fn stream_source(&self) -> &CuDFStreamSource {
        &self.__private_stream_source
    }
}
