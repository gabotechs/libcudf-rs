//! One-shot configuration for cuDF's process-global memory pools.
//!
//! [`ensure_pools_configured`] runs idempotently before the first cuDF
//! operation in this process and installs:
//!
//! - An RMM device-memory pool sized to most of the GPU's VRAM, so column
//!   buffers are sub-allocated from a single up-front `cudaMalloc` slab
//!   instead of paying a per-allocation `cudaMalloc`/`cudaFree` round trip.
//! - A cuDF pinned-host pool used by both the upload (`pin_record_batch`)
//!   and download (`to_arrow_host`) paths.
//! - A pinned-allocation threshold so cuDF's internal host allocations under
//!   the threshold draw from the pinned pool.
//!
//! No knobs are exposed: the pool is sized from the device's total VRAM at
//! init time, leaving a small reserve for the CUDA driver and downstream
//! kernel scratch space. If you need finer control (shared GPU, multi-tenant
//! workloads), call `cudf::set_current_device_resource` and
//! `cudf::config_default_pinned_memory_resource` yourself before the first
//! cuDF operation runs.

use crate::{CuDFError, Result};
use libcudf_sys::ffi;
use std::sync::OnceLock;

/// Fraction of total VRAM reserved for the device pool. The remainder is left
/// for the CUDA driver context, kernel scratch space, and any other CUDA
/// consumers in the process.
const DEVICE_POOL_VRAM_FRACTION_NUM: usize = 9;
const DEVICE_POOL_VRAM_FRACTION_DEN: usize = 10;

/// Page-locked host memory reserved by cuDF's pinned pool. 1 GiB comfortably
/// covers the working set of a multi-partition pipeline staging RecordBatches
/// to GPU. Pinned memory is page-locked host RAM, so this is a host-side
/// budget, not VRAM.
const PINNED_POOL_SIZE: usize = 1024 * 1024 * 1024;

/// cuDF host allocations at or below this size draw from the pinned pool;
/// larger ones stay pageable. 16 MiB lets short-lived per-batch scratch
/// (groupby keys, gather maps, intermediate offset buffers) come from the
/// pool while keeping multi-hundred-MB internal buffers off it.
const PINNED_THRESHOLD: usize = 16 * 1024 * 1024;

static CONFIGURATION: OnceLock<std::result::Result<(), String>> = OnceLock::new();

/// Apply the configuration globally for the current process. Idempotent: only
/// the first call has effect.
///
/// Crate-private. We call this implicitly at every public entry point that
/// creates GPU state (table/column/scalar constructors, pinned-buffer
/// allocation, etc.), so callers don't need to remember to invoke it.
pub(crate) fn ensure_pools_configured() -> Result<()> {
    match CONFIGURATION.get_or_init(configure_pools) {
        Ok(()) => Ok(()),
        Err(message) => Err(CuDFError::Configuration(message.clone())),
    }
}

fn configure_pools() -> std::result::Result<(), String> {
    // RMM requires the pool size to be a multiple of CUDA's allocation
    // alignment (256 bytes). Round down via a mask.
    const ALIGN: usize = 256;
    let memory = ffi::available_device_memory().map_err(|error| error.to_string())?;
    let memory = memory
        .as_ref()
        .ok_or_else(|| "RMM returned a null available-memory result".to_string())?;
    let device_pool_size = (memory.total_bytes() / DEVICE_POOL_VRAM_FRACTION_DEN
        * DEVICE_POOL_VRAM_FRACTION_NUM)
        & !(ALIGN - 1);
    let upstream = ffi::make_cuda_memory_resource();
    let upstream_ref = upstream
        .as_ref()
        .ok_or_else(|| "RMM returned a null CUDA memory resource".to_string())?;
    // `upstream` and `pool` are deliberately retained below for the
    // process lifetime.
    let pool = unsafe {
        ffi::make_pool_memory_resource_with_maximum(
            upstream_ref,
            device_pool_size,
            device_pool_size,
        )
    }
    .map_err(|error| error.to_string())?;
    let pool_ref = pool
        .as_ref()
        .ok_or_else(|| "RMM returned a null pool memory resource".to_string())?;
    let resource = unsafe { ffi::make_device_async_resource_ref(pool_ref) };
    let resource = resource
        .as_ref()
        .ok_or_else(|| "RMM returned a null device resource reference".to_string())?;
    // The returned handle owns the resource that was previously installed
    // (RMM's default `cuda_memory_resource`); dropping it only releases this
    // reference, not the pool just installed.
    drop(unsafe { ffi::set_current_device_resource_ref(resource) });
    // Process must keep the resource wrappers alive for the lifetime of
    // the program: cuDF stores a raw pointer back to them. Leaking them is
    // intentional and matches cuDF's own static-storage pattern.
    std::mem::forget(upstream);
    std::mem::forget(pool);

    let pinned = ffi::make_pinned_mr_options_with_pool_size(PINNED_POOL_SIZE);
    let pinned = pinned
        .as_ref()
        .ok_or_else(|| "cuDF returned null pinned-memory options".to_string())?;
    ffi::config_default_pinned_memory_resource(pinned);
    ffi::set_allocate_host_as_pinned_threshold(PINNED_THRESHOLD);
    Ok(())
}
