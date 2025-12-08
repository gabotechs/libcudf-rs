use crate::{arrow_type_to_cudf, CuDFColumnView, CuDFError};
use arrow::array::Array;
use arrow::ffi::FFI_ArrowArray;
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::ArrowError;
use cxx::UniquePtr;
use std::sync::Arc;

/// A GPU-accelerated column (similar to an Arrow Array)
///
/// This is a safe wrapper around cuDF's column type.
pub struct CuDFColumn {
    pub(crate) inner: UniquePtr<libcudf_sys::ffi::Column>,
}

impl CuDFColumn {
    pub fn new(inner: UniquePtr<libcudf_sys::ffi::Column>) -> Self {
        Self { inner }
    }

    /// Convert an Arrow array to a cuDF column
    ///
    /// This transfers the Arrow array data to GPU memory for processing with cuDF.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The Arrow array cannot be converted to cuDF format
    /// - There is insufficient GPU memory
    ///
    /// # Example
    ///
    /// ```no_run
    /// use arrow::array::{Int32Array, Array};
    /// use libcudf_rs::CuDFColumn;
    ///
    /// let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    /// let column = CuDFColumn::from_arrow_host(&array)?;
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn from_arrow_host(array: &dyn Array) -> Result<Self, CuDFError> {
        if arrow_type_to_cudf(array.data_type()).is_none() {
            return Err(CuDFError::ArrowError(ArrowError::NotYetImplemented(
                format!("Arrow type {} not supported in CuDF", array.data_type()),
            )));
        };

        let array_data = array.to_data();
        let ffi_array = FFI_ArrowArray::new(&array_data);
        let ffi_schema = FFI_ArrowSchema::try_from(array.data_type())?;

        let schema_ptr = &ffi_schema as *const FFI_ArrowSchema as *const u8;
        let array_ptr = &ffi_array as *const FFI_ArrowArray as *const u8;

        let inner = unsafe { libcudf_sys::ffi::column_from_arrow(schema_ptr, array_ptr) }?;
        Ok(Self { inner })
    }

    /// Return a [CuDFColumnView] pointing to this [CuDFColumn]. The current [CuDFColumn] will
    /// be kept alive at least until the [CuDFColumnView] is dropped.
    pub fn view(self: Arc<Self>) -> CuDFColumnView {
        let view = self.inner.view();
        CuDFColumnView::new_with_ref(view, Some(self))
    }

    /// Consumes the current [CuDFColumn], returning a [CuDFColumnView] pointing to it.
    pub fn into_view(self) -> CuDFColumnView {
        Arc::new(self).view()
    }

    /// Concatenate multiple [CuDFColumnView]s into a single [CuDFColumn].
    pub fn concat(views: Vec<CuDFColumnView>) -> Result<Self, CuDFError> {
        // Keep the references alive until the concat_column_views operation has completed.
        let mut _refs = Vec::with_capacity(views.len());
        let views = views
            .into_iter()
            .map(|x| {
                _refs.push(x._ref.clone());
                x.into_inner()
            })
            .collect::<Vec<_>>();
        Ok(Self::new(libcudf_sys::ffi::concat_column_views(&views)?))
    }
}
