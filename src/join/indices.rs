use crate::{CuDFColumnView, CuDFError, CuDFViewStorage};
use cxx::UniquePtr;
use libcudf_sys::ffi;
use std::sync::Arc;

/// Owning wrapper for device row indices returned by cuDF joins.
pub(crate) struct JoinIndexVector {
    inner: UniquePtr<ffi::DeviceIndexVector>,
}

impl JoinIndexVector {
    pub(super) fn new(inner: UniquePtr<ffi::DeviceIndexVector>) -> Self {
        Self { inner }
    }

    pub(super) fn len(&self) -> usize {
        self.inner.size()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(super) fn as_sys(&self) -> Result<&ffi::DeviceIndexVector, CuDFError> {
        self.inner
            .as_ref()
            .ok_or(CuDFError::NullHandle("device index vector"))
    }

    pub(super) fn view(self: Arc<Self>) -> CuDFColumnView {
        let view = self.inner.view();
        let storage: CuDFViewStorage = self;
        CuDFColumnView::from_view(view, Some(storage), None)
    }
}

pub(super) fn split_join_indices(
    mut indices: UniquePtr<ffi::JoinIndices>,
) -> (Arc<JoinIndexVector>, Arc<JoinIndexVector>) {
    (
        Arc::new(JoinIndexVector::new(indices.pin_mut().release_left())),
        Arc::new(JoinIndexVector::new(indices.pin_mut().release_right())),
    )
}

pub(super) fn split_hash_join_indices(
    mut indices: UniquePtr<ffi::HashJoinIndices>,
) -> (Arc<JoinIndexVector>, Arc<JoinIndexVector>) {
    (
        Arc::new(JoinIndexVector::new(indices.pin_mut().release_build())),
        Arc::new(JoinIndexVector::new(indices.pin_mut().release_probe())),
    )
}

pub(super) fn null_gather_index() -> i32 {
    ffi::join_no_match()
}
