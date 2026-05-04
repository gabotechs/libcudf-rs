/// Configuration for the process-wide RMM GPU device-memory pool.
///
/// Configure this before any cuDF operation to serve device allocations from a
/// reusable pool rather than calling `cudaMalloc`/`cudaFree` for every buffer.
/// See [`configure_default_pools`](crate::configure_default_pools) for default
/// values.
///
/// ## Without a device pool
///
/// Device buffers are allocated from CUDA directly. Workloads that create many
/// batches can pay a driver allocation/free cost for each column buffer.
///
/// ```text
/// batch N:
///   col 0  [cudaMalloc] [copy/compute ----------------] [cudaFree]
///   col 1               [cudaMalloc] [copy/compute ---] [cudaFree]
///   col 2                            [cudaMalloc] [---] [cudaFree]
/// ```
///
/// ## With a device pool
///
/// The pool reserves `initial_size` once. Allocations are then served from
/// reusable pool blocks. If the pool needs more memory, it can grow until
/// `max_size`.
///
/// ```text
/// startup:
///   [cudaMalloc initial pool ------------------------------------------]
///
/// batch N:
///   [pool alloc col0] [pool alloc col1] [pool alloc col2]
///   [copy/compute -----------------------------------------------------]
///   [pool free col0]  [pool free col1]  [pool free col2]
///
/// batch N+1:
///   [reuse pool blocks -----------------------------------------------]
/// ```
///
/// This is most useful for repeated GPU pipelines where allocation churn is a
/// measurable part of runtime. Size the pool conservatively on shared GPUs.
pub struct DevicePoolConfig {
    /// Bytes of GPU VRAM to reserve upfront via a single `cudaMalloc`.
    ///
    /// The pool pre-allocates this slab when [`DevicePoolConfig::apply`] is called.
    /// If the slab fills up, it grows until [`DevicePoolConfig::max_size`] is reached.
    ///
    /// Set to at least your pipeline's peak working set (the sum of all live column
    /// buffers across concurrent batches). Sizing below this causes growth pauses during
    /// warm-up.
    pub initial_size: usize,

    /// Hard cap in bytes on total pool VRAM.
    ///
    /// The pool will not grow beyond this. If an allocation would exceed it,
    /// cuDF throws `std::bad_alloc`. Set this to leave headroom for other GPU
    /// workloads sharing the same device.
    ///
    /// Must be at least `initial_size`.
    pub max_size: usize,
}

impl Default for DevicePoolConfig {
    /// Conservative defaults suitable for GPU analytics workloads.
    ///
    /// - `initial_size`: 512 MB
    /// - `max_size`: 4 GB
    ///
    /// Override `initial_size` upward if your pipeline's peak working set exceeds
    /// 512 MB to avoid growth pauses during warm-up.
    fn default() -> Self {
        Self {
            initial_size: 512 * 1024 * 1024,  // 512 MB
            max_size: 4 * 1024 * 1024 * 1024, //   4 GB
        }
    }
}

impl DevicePoolConfig {
    /// Apply this configuration globally for the current process.
    ///
    /// Must be called before any cuDF operations run.
    ///
    /// Returns `true` if the pool was configured, `false` if already set by
    /// a previous call.
    pub fn apply(&self) -> bool {
        libcudf_sys::ffi::config_device_memory_pool(self.initial_size, self.max_size)
    }
}
