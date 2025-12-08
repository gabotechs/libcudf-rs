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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;

    #[test]
    fn test_column_from_arrow_int32() {
        let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let column = CuDFColumn::from_arrow_host(&array)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        assert_eq!(column.len(), 5);
        assert!(!column.is_empty());
    }

    #[test]
    fn test_column_from_arrow_string() {
        let array = StringArray::from(vec!["hello", "world", "test"]);
        let column = CuDFColumn::from_arrow_host(&array)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        assert_eq!(column.len(), 3);
        assert!(!column.is_empty());
    }

    #[test]
    fn test_column_from_arrow_with_nulls() {
        let array = Int32Array::from(vec![Some(1), None, Some(3), None, Some(5)]);
        let column = CuDFColumn::from_arrow_host(&array)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        assert_eq!(column.len(), 5);
        assert!(!column.is_empty());
    }

    #[test]
    fn test_to_arrow_host_int64() {
        let original = Int64Array::from(vec![100, 200, 300, 400, 500]);
        let column = CuDFColumn::from_arrow_host(&original)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        let result = column
            .to_arrow_host()
            .expect("Failed to convert column back to Arrow");

        assert_eq!(result.len(), 5);
        let result_int64 = result
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Failed to downcast to Int64Array");

        for i in 0..5 {
            assert_eq!(result_int64.value(i), original.value(i));
        }
    }

    #[test]
    fn test_to_arrow_host_float64() {
        let original = Float64Array::from(vec![1.5, 2.5, 3.5, 4.5, 5.5]);
        let column = CuDFColumn::from_arrow_host(&original)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        let result = column
            .to_arrow_host()
            .expect("Failed to convert column back to Arrow");

        assert_eq!(result.len(), 5);
        let result_float64 = result
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Failed to downcast to Float64Array");

        for i in 0..5 {
            assert_eq!(result_float64.value(i), original.value(i));
        }
    }

    #[test]
    fn test_to_arrow_host_string() {
        let original = StringArray::from(vec!["hello", "world", "test", "cudf", "rust"]);
        let column = CuDFColumn::from_arrow_host(&original)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        let result = column
            .to_arrow_host()
            .expect("Failed to convert column back to Arrow");

        assert_eq!(result.len(), 5);
        let result_string = result
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast to StringArray");

        for i in 0..5 {
            assert_eq!(result_string.value(i), original.value(i));
        }
    }

    #[test]
    fn test_to_arrow_host_with_nulls() {
        let original = Int32Array::from(vec![Some(1), None, Some(3), None, Some(5)]);
        let column = CuDFColumn::from_arrow_host(&original)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        let result = column
            .to_arrow_host()
            .expect("Failed to convert column back to Arrow");

        assert_eq!(result.len(), 5);
        let result_int32 = result
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Failed to downcast to Int32Array");

        assert!(result_int32.is_valid(0));
        assert_eq!(result_int32.value(0), 1);

        assert!(result_int32.is_null(1));

        assert!(result_int32.is_valid(2));
        assert_eq!(result_int32.value(2), 3);

        assert!(result_int32.is_null(3));

        assert!(result_int32.is_valid(4));
        assert_eq!(result_int32.value(4), 5);
    }

    #[test]
    fn test_to_arrow_host_empty() {
        let original = Int32Array::from(Vec::<i32>::new());
        let column = CuDFColumn::from_arrow_host(&original)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        let result = column
            .to_arrow_host()
            .expect("Failed to convert empty column back to Arrow");

        assert_eq!(result.len(), 0);
        assert!(result.is_empty());
    }

    #[test]
    fn test_to_arrow_host_roundtrip_preserves_data() {
        let original = Int32Array::from(vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100]);
        let column = CuDFColumn::from_arrow_host(&original)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        let result = column
            .to_arrow_host()
            .expect("Failed to convert column back to Arrow");

        let result_int32 = result
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Failed to downcast to Int32Array");

        assert_eq!(result_int32.len(), original.len());
        for i in 0..original.len() {
            assert_eq!(result_int32.value(i), original.value(i));
        }
    }
}
