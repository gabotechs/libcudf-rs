use datafusion::common::extensions_options;
use datafusion::config::ConfigExtension;

extensions_options! {
    pub struct CuDFConfig {
        /// Enables CuDF optimizations.
        pub enable: bool, default = false
    }
}

impl ConfigExtension for CuDFConfig {
    const PREFIX: &'static str = "cudf";
}
