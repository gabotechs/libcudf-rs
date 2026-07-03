use crate::{CuDFError, CuDFTableView};
use libcudf_sys::{ffi, NullEquality};

/// Controls whether null join-key values compare equal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CuDFNullEquality {
    /// Null join-key values match other null join-key values.
    Equal,
    /// Null join-key values do not match anything.
    Unequal,
}

impl CuDFNullEquality {
    pub(super) fn into_sys(self) -> NullEquality {
        match self {
            Self::Equal => NullEquality::Equal,
            Self::Unequal => NullEquality::Unequal,
        }
    }
}

/// Resolves optional output-column selections while keeping selected views alive.
pub(super) struct SelectedPayloads<'a> {
    left_selected: Option<CuDFTableView>,
    right_selected: Option<CuDFTableView>,
    left_fallback: &'a CuDFTableView,
    right_fallback: &'a CuDFTableView,
}

impl<'a> SelectedPayloads<'a> {
    pub(super) fn new(
        left: &'a CuDFTableView,
        right: &'a CuDFTableView,
        left_cols: Option<&[usize]>,
        right_cols: Option<&[usize]>,
    ) -> Result<Self, CuDFError> {
        Ok(Self {
            left_selected: left_cols
                .map(|cols| left.select_columns(cols))
                .transpose()?,
            right_selected: right_cols
                .map(|cols| right.select_columns(cols))
                .transpose()?,
            left_fallback: left,
            right_fallback: right,
        })
    }

    pub(super) fn left_view(&self) -> &CuDFTableView {
        self.left_selected.as_ref().unwrap_or(self.left_fallback)
    }

    pub(super) fn right_view(&self) -> &CuDFTableView {
        self.right_selected.as_ref().unwrap_or(self.right_fallback)
    }

    pub(super) fn left(&self) -> Result<&ffi::TableView, CuDFError> {
        table_ref(self.left_view())
    }

    pub(super) fn right(&self) -> Result<&ffi::TableView, CuDFError> {
        table_ref(self.right_view())
    }
}

pub(super) fn table_ref(view: &CuDFTableView) -> Result<&ffi::TableView, CuDFError> {
    view.inner()
        .as_ref()
        .ok_or(CuDFError::NullHandle("table view"))
}
