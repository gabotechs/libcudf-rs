use datafusion::common::extensions_options;
use datafusion::config::ConfigExtension;

extensions_options! {
    pub struct CuDFConfig {
        /// Enables CuDF optimizations.
        pub enable: bool, default = false
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
