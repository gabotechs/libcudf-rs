use datafusion::common::{extensions_options, plan_err, DataFusionError};
use datafusion::config::{ConfigExtension, ConfigOptions};

pub(crate) const DEFAULT_PARQUET_SCAN_FILES_PER_BATCH: usize = 8;

extensions_options! {
    pub struct CuDFConfig {
        /// Enables CuDF optimizations.
        pub enable: bool, default = true
        /// Target input bytes accumulated by each cuDF aggregate chunk before flushing.
        pub aggregate_chunk_target_bytes: usize, default = 256 * 1024 * 1024
        /// Target row count for batches uploaded to the GPU. Small upstream batches are
        /// coalesced per partition up to this size to amortize cuDF's per-batch
        /// kernel-launch overhead.
        pub batch_size: usize, default = 512_000
        /// Enables experimental local Parquet file scans with cuDF-backed scans.
        pub parquet_scan: bool, default = false
        /// Maximum number of files included in each cuDF Parquet read.
        pub parquet_scan_files_per_batch: usize, default = DEFAULT_PARQUET_SCAN_FILES_PER_BATCH
    }
}

impl ConfigExtension for CuDFConfig {
    const PREFIX: &'static str = "cudf";
}

impl CuDFConfig {
    /// Return a copy with cuDF optimizations enabled or disabled.
    #[must_use]
    pub fn with_enable(mut self, enable: bool) -> Self {
        self.enable = enable;
        self
    }

    /// Return a copy with the aggregate chunk target size set.
    #[must_use]
    pub fn with_aggregate_chunk_target_bytes(mut self, bytes: usize) -> Self {
        self.aggregate_chunk_target_bytes = bytes;
        self
    }

    /// Return a copy with the GPU upload batch row target set.
    #[must_use]
    pub fn with_batch_size(mut self, rows: usize) -> Self {
        self.batch_size = rows;
        self
    }

    /// Return a copy with direct cuDF Parquet scans enabled or disabled.
    #[must_use]
    pub fn with_parquet_scan(mut self, parquet_scan: bool) -> Self {
        self.parquet_scan = parquet_scan;
        self
    }

    /// Return a copy with the maximum files per cuDF Parquet read set.
    #[must_use]
    pub fn with_parquet_scan_files_per_batch(mut self, files_per_batch: usize) -> Self {
        self.parquet_scan_files_per_batch = files_per_batch;
        self
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
