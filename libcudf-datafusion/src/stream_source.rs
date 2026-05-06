use datafusion::common::{not_impl_err, Result};
use datafusion::config::{ConfigField, Visit};
use libcudf_rs::{CuDFStream, CuDFStreamFlags};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Allocates CUDA streams for cuDF execution.
///
/// Lives inside [`crate::CuDFConfig`] as private runtime state. Not a
/// user-facing config surface — the [`ConfigField`] impl is a stub so the
/// config-options macro accepts the type.
#[derive(Clone, Default, PartialEq)]
pub(crate) struct CuDFStreamSource;

impl CuDFStreamSource {
    pub(crate) fn allocate(&self) -> Arc<CuDFStream> {
        Arc::new(CuDFStream::with_flags(CuDFStreamFlags::NonBlocking))
    }
}

impl ConfigField for CuDFStreamSource {
    fn visit<V: Visit>(&self, _: &mut V, _: &str, _: &'static str) {}

    fn set(&mut self, _: &str, _: &str) -> Result<()> {
        not_impl_err!("CuDFStreamSource is not user-configurable")
    }
}

impl Debug for CuDFStreamSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CuDFStreamSource")
    }
}
