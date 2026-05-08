use cxx::UniquePtr;
use libcudf_sys::ffi;

use crate::{CuDFError, Result};

/// Return a non-null device resource reference from a cuDF FFI handle.
///
/// cuDF should always return a valid device resource ref; this surfaces a Rust
/// error if the FFI handle is unexpectedly null.
pub(crate) fn resource_ref(
    resource: &UniquePtr<ffi::DeviceAsyncResourceRef>,
) -> Result<&ffi::DeviceAsyncResourceRef> {
    resource
        .as_ref()
        .ok_or(CuDFError::NullHandle("current device resource ref"))
}
