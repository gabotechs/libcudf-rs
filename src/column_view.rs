use crate::cudf_reference::CuDFRef;
use crate::{cudf_type_to_arrow, CuDFError};
use arrow::array::{Array, ArrayData, ArrayRef};
use arrow::buffer::NullBuffer;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_schema::DataType;
use cxx::UniquePtr;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

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
}

impl CuDFColumnView {
    /// Create a CuDFColumnView from an existing cuDF column view
    ///
    /// This creates a non-owning view. The caller must ensure the underlying
    /// column data remains valid for the lifetime of this view.
    pub fn new(view: UniquePtr<libcudf_sys::ffi::ColumnView>) -> Self {
        let cudf_dtype = view.data_type();
        let dt = cudf_type_to_arrow(cudf_dtype.id());
        let dt = dt.unwrap_or(DataType::Null);
        Self {
            _ref: None,
            inner: view,
            dt,
        }
    }

    /// Create a [CuDFColumnView] from a column view and optional table reference
    ///
    /// This is used internally to create column views that keep the source table or column alive
    pub(crate) fn new_with_ref(
        inner: UniquePtr<libcudf_sys::ffi::ColumnView>,
        _ref: Option<Arc<dyn CuDFRef>>,
    ) -> Self {
        let cudf_dtype = inner.data_type();
        let dt = cudf_type_to_arrow(cudf_dtype.id());
        let dt = dt.unwrap_or(DataType::Null);
        Self { _ref, inner, dt }
    }

    pub fn inner(&self) -> &UniquePtr<libcudf_sys::ffi::ColumnView> {
        &self.inner
    }

    /// Consume this wrapper and return the underlying cuDF column view
    pub fn into_inner(self) -> UniquePtr<libcudf_sys::ffi::ColumnView> {
        self.inner
    }

    /// Get the raw device pointer to the column's data
    ///
    /// This returns a pointer to GPU device memory. The pointer is only valid
    /// as long as this CuDFColumn exists.
    ///
    /// # Safety
    ///
    /// This is marked unsafe because:
    /// - The returned pointer points to GPU device memory
    /// - Dereferencing it from CPU code will cause undefined behavior
    /// - Use CUDA APIs to interact with this pointer
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use arrow::array::Int32Array;
    /// use libcudf_rs::{CuDFColumn, CuDFColumnView};
    ///
    /// let array = Int32Array::from(vec![1, 2, 3]);
    /// let column = CuDFColumn::from_arrow_host(&array)?.into_view();
    /// let gpu_ptr = unsafe { column.data_ptr() };
    /// // Use CUDA APIs to work with gpu_ptr
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub unsafe fn data_ptr(&self) -> u64 {
        self.inner.data_ptr()
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
        let mut device_array = libcudf_sys::ArrowDeviceArray {
            array: FFI_ArrowArray::empty(),
            device_id: -1,
            device_type: 1, // CPU
            sync_event: std::ptr::null_mut(),
            reserved: [0; 3],
        };

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
        ArrayData::new_empty(&self.dt)
    }

    fn into_data(self) -> ArrayData {
        ArrayData::new_empty(&self.dt)
    }

    fn data_type(&self) -> &DataType {
        &self.dt
    }

    fn slice(&self, _offset: usize, _length: usize) -> ArrayRef {
        todo!()
    }

    fn len(&self) -> usize {
        self.inner.size()
    }

    fn is_empty(&self) -> bool {
        self.inner.size() == 0
    }

    fn offset(&self) -> usize {
        todo!()
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        todo!()
    }

    fn get_buffer_memory_size(&self) -> usize {
        todo!()
    }

    fn get_array_memory_size(&self) -> usize {
        todo!()
    }
}
