use crate::cudf_reference::CuDFRef;
use crate::data_type::cudf_type_to_arrow;
use crate::{slice_column, CuDFError};
use arrow::array::{Array, ArrayData, ArrayRef};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow::ffi::FFI_ArrowSchema;
use arrow_schema::DataType;
use cxx::UniquePtr;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, OnceLock};

/// A view into a cuDF column stored in GPU memory
///
/// This type wraps a cuDF column_view and implements Arrow's Array trait,
/// allowing cuDF columns to be used seamlessly with the Arrow ecosystem.
///
/// CuDFColumnView is a non-owning view that may optionally keep the underlying
/// column alive. It can be created from Arrow arrays (copying to GPU) or from
/// existing cuDF columns.
pub struct CuDFColumnView {
    // Keep a ref to CuDF structs so that they live as long as this view exists
    pub(crate) _ref: Option<Arc<dyn CuDFRef>>,
    inner: UniquePtr<libcudf_sys::ffi::ColumnView>,
    dt: DataType,
    null_buf: OnceLock<Option<NullBuffer>>,
}

impl CuDFColumnView {
    /// Create a [CuDFColumnView] from a column view and optional table reference
    ///
    /// This is used internally to create column views that keep the source table or column alive
    pub(crate) fn new_with_ref(
        inner: UniquePtr<libcudf_sys::ffi::ColumnView>,
        _ref: Option<Arc<dyn CuDFRef>>,
    ) -> Self {
        let cudf_dtype = inner.data_type();
        let dt = cudf_type_to_arrow(&cudf_dtype);
        let dt = dt.unwrap_or(DataType::Null);
        Self {
            _ref,
            inner,
            dt,
            null_buf: OnceLock::new(),
        }
    }

    pub(crate) fn inner(&self) -> &UniquePtr<libcudf_sys::ffi::ColumnView> {
        &self.inner
    }

    /// Consume this wrapper and return the underlying cuDF column view
    pub(crate) fn into_inner(self) -> UniquePtr<libcudf_sys::ffi::ColumnView> {
        self.inner
    }

    /// Create a view with a different Arrow `DataType` label, sharing the same GPU memory.
    ///
    /// The caller must ensure the raw GPU values are correct for the new type.
    /// Safe to use when the view is only unloaded to host, not chained into further GPU arithmetic.
    pub fn with_data_type(self, dt: DataType) -> Self {
        Self {
            _ref: self._ref,
            inner: self.inner,
            dt,
            null_buf: self.null_buf,
        }
    }

    /// Convert the cuDF column view to an Arrow array, copying data from GPU to host
    ///
    /// This method copies the GPU data back to the CPU and creates an Arrow array.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The cuDF column cannot be converted to Arrow format
    /// - There is an error copying data from GPU to host
    ///
    /// # Example
    ///
    /// ```no_run
    /// use arrow::array::Int32Array;
    /// use libcudf_rs::CuDFColumn;
    ///
    /// let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    /// let column = CuDFColumn::from_arrow_host(&array)?.into_view();
    /// // Do some GPU processing...
    /// let result = column.to_arrow_host()?;
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn to_arrow_host(&self) -> Result<ArrayRef, CuDFError> {
        let mut device_array = libcudf_sys::ArrowDeviceArray::new_cpu();

        // Create schema from the column's data type
        let ffi_schema = FFI_ArrowSchema::try_from(self.data_type())?;

        // Convert the column view to Arrow format (copying from GPU to host)
        // cuDF's to_arrow_array expects an ArrowDeviceArray pointer
        unsafe {
            let device_array_ptr =
                &mut device_array as *mut libcudf_sys::ArrowDeviceArray as *mut u8;
            self.inner.to_arrow_array(device_array_ptr);
        }

        // Convert from FFI structures to Arrow ArrayData
        // Extract just the ArrowArray part
        let array_data = unsafe { arrow::ffi::from_ffi(device_array.array, &ffi_schema)? };

        // Create an ArrayRef from the ArrayData
        Ok(arrow::array::make_array(array_data))
    }
}

impl Clone for CuDFColumnView {
    fn clone(&self) -> Self {
        // Clone the view using the FFI clone method
        let cloned_inner = self.inner.clone();
        Self {
            _ref: self._ref.clone(),
            inner: cloned_inner,
            dt: self.dt.clone(),
            null_buf: self.null_buf.clone(),
        }
    }
}

impl Debug for CuDFColumnView {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CuDFColumnView: type={}, size={}",
            self.data_type(),
            self.len()
        )
    }
}

impl Array for CuDFColumnView {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_data(&self) -> ArrayData {
        // In debug/test builds this panics so implicit GPU->CPU copies are easily
        // caught.
        //
        // A call to_arrow_host() explicitly rather than this method to separate implicit
        // and explicit transfers.
        debug_assert!(
            false,
            "CuDFColumnView::to_data(), implicit GPU→CPU copy detected. \
             Call to_arrow_host() explicitly."
        );
        self.to_arrow_host()
            .expect("Failed to convert GPU column to host Arrow")
            .to_data()
    }

    fn into_data(self) -> ArrayData {
        self.to_data()
    }

    fn data_type(&self) -> &DataType {
        &self.dt
    }

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(slice_column(self, offset, length).expect("Failed to slice column"))
    }

    fn len(&self) -> usize {
        self.inner.size()
    }

    fn is_empty(&self) -> bool {
        self.inner.size() == 0
    }

    fn offset(&self) -> usize {
        self.inner.offset() as usize
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.null_buf
            .get_or_init(|| {
                if self.null_count() == 0 {
                    return None;
                }
                let null_bytes = self.inner().get_null_buffer();
                let offset = self.inner().offset() as usize;
                let length = self.inner().size();

                let buffer = Buffer::from_vec(null_bytes);
                let boolean_buffer = BooleanBuffer::new(buffer, offset, length);
                Some(NullBuffer::new(boolean_buffer))
            })
            .as_ref()
    }

    fn get_buffer_memory_size(&self) -> usize {
        self.inner().get_buffer_memory_size()
    }

    fn get_array_memory_size(&self) -> usize {
        self.inner().get_array_memory_size()
    }

    fn logical_nulls(&self) -> Option<NullBuffer> {
        // TODO: For now only primitive types are supported
        // In this case logical_nulls == physical_nulls
        self.nulls().cloned()
    }

    fn is_null(&self, index: usize) -> bool {
        match self.nulls() {
            Some(nulls) => nulls.is_null(index),
            None => false,
        }
    }

    fn is_valid(&self, index: usize) -> bool {
        !self.is_null(index)
    }

    fn null_count(&self) -> usize {
        self.inner.null_count() as usize
    }

    fn logical_null_count(&self) -> usize {
        // TODO: For now only primitive types are supported
        // In this case logical_null_count == physical_null_count
        self.null_count()
    }

    fn is_nullable(&self) -> bool {
        // All cuDF columns can potentially have nulls
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CuDFColumn;
    use arrow::array::Int32Array;

    #[test]
    fn test_column_view_clone() {
        let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let column = CuDFColumn::from_arrow_host(&array)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        let cloned = column.clone();

        assert_eq!(column.len(), 5);
        assert_eq!(cloned.len(), 5);

        let original_ptr = column.inner.data_ptr();
        let cloned_ptr = cloned.inner.data_ptr();
        assert_eq!(
            original_ptr, cloned_ptr,
            "Cloned view should point to the same GPU memory"
        );
    }

    #[test]
    fn test_multiple_clones() {
        let array = Int32Array::from(vec![10, 20, 30]);
        let column = CuDFColumn::from_arrow_host(&array)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        let clone1 = column.clone();
        let clone2 = column.clone();
        let clone3 = clone1.clone();

        let ptr = column.inner.data_ptr();
        assert_eq!(clone1.inner.data_ptr(), ptr);
        assert_eq!(clone2.inner.data_ptr(), ptr);
        assert_eq!(clone3.inner.data_ptr(), ptr);

        assert_eq!(column.len(), 3);
        assert_eq!(clone1.len(), 3);
        assert_eq!(clone2.len(), 3);
        assert_eq!(clone3.len(), 3);

        drop(column);
        assert_eq!(clone1.inner.data_ptr(), ptr);
        assert_eq!(clone1.len(), 3);
    }

    #[test]
    fn test_column_data_is_on_gpu() {
        let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let column = CuDFColumn::from_arrow_host(&array)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        let ptr = column.inner.data_ptr();

        assert_ne!(ptr, 0, "Column data pointer should not be null");

        let mut attrs: cuda_runtime_sys::cudaPointerAttributes = unsafe { std::mem::zeroed() };
        let result = unsafe {
            cuda_runtime_sys::cudaPointerGetAttributes(
                &mut attrs as *mut _,
                ptr as *const std::ffi::c_void,
            )
        };

        assert_eq!(
            result,
            cuda_runtime_sys::cudaError_t::cudaSuccess,
            "cudaPointerGetAttributes should succeed"
        );

        assert_ne!(
            attrs.type_,
            cuda_runtime_sys::cudaMemoryType::cudaMemoryTypeHost,
            "Column data should NOT be in host memory"
        );

        match attrs.type_ {
            cuda_runtime_sys::cudaMemoryType::cudaMemoryTypeDevice => {}
            cuda_runtime_sys::cudaMemoryType::cudaMemoryTypeUnregistered => {}
            _ => {
                panic!("Unexpected memory type: {:?}", attrs.type_);
            }
        }
    }

    #[test]
    fn test_get_buffer_memory_size_int32() {
        let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let column = CuDFColumn::from_arrow_host(&array)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        let size = column.get_buffer_memory_size();
        assert_eq!(size, 20, "Int32 column should be 20 bytes");
    }

    #[test]
    fn test_get_buffer_memory_size_large() {
        let data: Vec<i32> = (0..1_000_000).collect();
        let array = Int32Array::from(data);
        let column = CuDFColumn::from_arrow_host(&array)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        let size = column.get_buffer_memory_size();
        assert_eq!(size, 4_000_000, "1M Int32 elements should be 4MB");
    }

    #[test]
    fn test_get_array_memory_size_no_nulls() {
        let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let column = CuDFColumn::from_arrow_host(&array)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        let buffer_size = column.get_buffer_memory_size();
        let array_size = column.get_array_memory_size();

        // With no nulls, array_size should equal buffer_size
        assert_eq!(
            array_size, buffer_size,
            "Array size should equal buffer size when no nulls"
        );
    }

    #[test]
    fn test_get_array_memory_size_with_nulls() {
        let array = Int32Array::from(vec![Some(1), None, Some(3), None, Some(5)]);
        let column = CuDFColumn::from_arrow_host(&array)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        let buffer_size = column.get_buffer_memory_size();
        let array_size = column.get_array_memory_size();

        // With nulls -> array_size = buffer_size + null_mask_size
        assert!(
            array_size > buffer_size,
            "Array size should be larger than buffer size when nulls present"
        );

        let null_overhead = array_size - buffer_size;
        assert!(
            (1..=128).contains(&null_overhead),
            "Null mask overhead should be between 2 and 128 bytes, got {}",
            null_overhead
        );
    }

    #[test]
    fn test_nulls_no_nulls_returns_none() {
        let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let column = CuDFColumn::from_arrow_host(&array)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        let nulls = column.nulls();
        assert!(nulls.is_none(), "Column with no nulls should return None");
    }

    #[test]
    fn test_nulls_with_nulls_returns_buffer() {
        let array = Int32Array::from(vec![Some(1), None, Some(3), None, Some(5)]);
        let column = CuDFColumn::from_arrow_host(&array)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        let nulls = column.nulls();
        assert!(
            nulls.is_some(),
            "Column with nulls should return Some(NullBuffer)"
        );
    }

    #[test]
    fn test_nulls_correct_bit_pattern() {
        let array = Int32Array::from(vec![Some(10), None, Some(30), None, Some(50)]);
        let column = CuDFColumn::from_arrow_host(&array)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        let nulls = column.nulls().expect("Should have null buffer");
        assert!(nulls.is_valid(0), "Element 0 should be valid (Some(10))");
        assert!(nulls.is_null(1), "Element 1 should be null");
        assert!(nulls.is_valid(2), "Element 2 should be valid (Some(30))");
        assert!(nulls.is_null(3), "Element 3 should be null");
        assert!(nulls.is_valid(4), "Element 4 should be valid (Some(50))");
    }

    #[test]
    fn test_nulls_cached_same_reference() {
        let array = Int32Array::from(vec![Some(1), None, Some(3)]);
        let column = CuDFColumn::from_arrow_host(&array)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        let nulls1 = column.nulls();
        let nulls2 = column.nulls();

        // Should return references to the same cached NullBuffer
        assert!(nulls1.is_some());
        assert!(nulls2.is_some());

        let ptr1 = nulls1.unwrap() as *const _;
        let ptr2 = nulls2.unwrap() as *const _;
        assert_eq!(
            ptr1, ptr2,
            "Subsequent calls should return same cached buffer"
        );
    }

    #[test]
    fn test_is_null_uses_null_buffer() {
        let array = Int32Array::from(vec![Some(1), None, Some(3), None, Some(5)]);
        let column = CuDFColumn::from_arrow_host(&array)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        assert!(!column.is_null(0), "Index 0 should not be null");
        assert!(column.is_null(1), "Index 1 should be null");
        assert!(!column.is_null(2), "Index 2 should not be null");
        assert!(column.is_null(3), "Index 3 should be null");
        assert!(!column.is_null(4), "Index 4 should not be null");
    }

    #[test]
    fn test_is_valid_inverse_of_is_null() {
        let array = Int32Array::from(vec![Some(1), None, Some(3)]);
        let column = CuDFColumn::from_arrow_host(&array)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        for i in 0..3 {
            assert_eq!(
                column.is_valid(i),
                !column.is_null(i),
                "is_valid should be inverse of is_null at index {}",
                i
            );
        }
    }

    #[test]
    fn test_null_count_matches_null_buffer() {
        let array = Int32Array::from(vec![Some(1), None, Some(3), None, Some(5), None]);
        let column = CuDFColumn::from_arrow_host(&array)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        let null_count = column.null_count();

        // Should have 3 nulls (indices 1, 3, 5)
        assert_eq!(null_count, 3, "Should have 3 nulls");

        let manual_count = (0..column.len()).filter(|&i| column.is_null(i)).count();
        assert_eq!(
            null_count, manual_count,
            "null_count() should match manual count"
        );
    }

    #[test]
    fn test_nulls_with_large_column() {
        let data: Vec<Option<i32>> = (0..10_000)
            .map(|i| if i % 2 == 0 { Some(i) } else { None })
            .collect();
        let array = Int32Array::from(data);
        let column = CuDFColumn::from_arrow_host(&array)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        let nulls = column.nulls().expect("Should have null buffer");

        // Verify pattern: even indices valid, odd indices null
        for i in 0..100 {
            if i % 2 == 0 {
                assert!(nulls.is_valid(i), "Even index {} should be valid", i);
            } else {
                assert!(nulls.is_null(i), "Odd index {} should be null", i);
            }
        }

        assert_eq!(column.null_count(), 5_000, "Should have 5,000 nulls");
    }

    #[test]
    fn test_logical_nulls_equals_physical_nulls() {
        // For primitive types, logical_nulls should equal physical nulls
        let array = Int32Array::from(vec![Some(1), None, Some(3)]);
        let column = CuDFColumn::from_arrow_host(&array)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        let physical = column.nulls();
        let logical = column.logical_nulls();

        match (physical, logical) {
            (Some(p), Some(l)) => {
                // Both should have same null pattern
                for i in 0..3 {
                    assert_eq!(
                        p.is_null(i),
                        l.is_null(i),
                        "Physical and logical nulls should match at index {}",
                        i
                    );
                }
            }
            (None, None) => {
                // Both None is valid
            }
            _ => {
                panic!("Physical and logical nulls should both be Some or both None");
            }
        }
    }
}
