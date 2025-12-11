use datafusion::common::extensions_options;
use datafusion::config::ConfigExtension;

extensions_options! {
    pub struct CuDFConfig {
        /// Enables CuDF optimizations.
        pub enable: bool, default = false
        /// Batch size for moving data from CPU to GPU and vice-versa.
        pub batch_size: usize, default = 8192 * 10
    }
}

impl ConfigExtension for CuDFConfig {
    const PREFIX: &'static str = "cudf";
}
