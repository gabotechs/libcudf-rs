/// Configuration for cuDF's process-wide pinned host-memory pool.
///
/// Configure this before any cuDF operation to let eligible cuDF host
/// allocations come from a reusable page-locked pool. See
/// [`configure_default_pools`](crate::configure_default_pools) for default
/// values.
///
/// cuDF uses `threshold` as a maximum allocation size for pinned host memory:
/// allocations at or below the threshold use pinned memory, while larger
/// allocations use pageable memory. The pool backs those eligible pinned
/// allocations.
///
/// ## Pageable host allocation
///
/// Pageable host memory may require CUDA to stage data through temporary pinned
/// memory before DMA can proceed.
///
/// ```text
/// CPU pageable buffer
///   [stage chunk] [wait] [stage chunk] [wait] [stage chunk] [wait]
///
/// CUDA DMA
///                 [copy]              [copy]              [copy]
/// ```
///
/// ## Pinned host allocation
///
/// Eligible cuDF host allocations are page-locked and can be used directly for
/// transfer. The exact copy path still depends on the cuDF operation and source
/// or destination buffers, but the allocation itself comes from the reusable
/// pinned pool.
///
/// ```text
/// startup:
///   [cudaMallocHost pinned pool ---------------------------------------]
///
/// cuDF host allocation <= threshold:
///   [pool alloc] [DMA-capable host buffer -----------------------------]
///   [pool free]
///
/// cuDF host allocation > threshold:
///   [pageable allocation ---------------------------------------------]
/// ```
///
/// This primarily helps paths where cuDF owns internal host buffers, such as
/// GPU-to-host conversion and staging work. It does not pin user-owned Arrow
/// input buffers automatically.
pub struct PinnedPoolConfig {
    /// Bytes of page-locked host RAM to reserve upfront.
    ///
    /// The pool is fixed-size, both the initial and maximum capacity are set to this
    /// value. If exhausted, cuDF falls back to a non-pooled pinned allocation rather
    /// than growing or erroring.
    ///
    /// Paid once at [`PinnedPoolConfig::apply`] time. Eligible cuDF internal host
    /// allocations draw from this slab.
    ///
    /// Pinned memory is page-locked and cannot be swapped, so it reduces the
    /// pageable RAM available to the rest of the system. Size conservatively on
    /// shared or memory-constrained hosts.
    pub pool_size: usize,

    /// Largest allocation size, in bytes, that uses pinned host memory.
    ///
    /// cuDF's built-in default is 0. Allocations larger than this threshold use
    /// pageable memory.
    pub threshold: usize,
}

impl Default for PinnedPoolConfig {
    /// Conservative defaults suitable for GPU analytics workloads.
    ///
    /// - `pool_size`: 512 MB
    /// - `threshold`: 1 MB
    ///
    /// Override `pool_size` upward if your batches regularly exceed 256 MB per column,
    /// or downward on memory-constrained hosts.
    fn default() -> Self {
        Self {
            pool_size: 512 * 1024 * 1024, // 512 MB
            threshold: 1 * 1024 * 1024,   //   1 MB
        }
    }
}

impl PinnedPoolConfig {
    /// Apply this configuration globally for the current process.
    ///
    /// Must be called before any cuDF operations run.
    ///
    /// Returns `true` if the pool was configured, `false` if already set by
    /// a previous call (the threshold is still updated).
    pub fn apply(&self) -> bool {
        let configured = libcudf_sys::ffi::config_pinned_memory_resource(self.pool_size);
        libcudf_sys::ffi::set_host_pinned_threshold(self.threshold);
        configured
    }
}
