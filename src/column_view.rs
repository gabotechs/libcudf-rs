use crate::{cudf_type_to_arrow, CuDFError};
use arrow::array::{Array, ArrayData, ArrayRef};
use arrow::buffer::NullBuffer;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_schema::{ArrowError, DataType};
use cxx::UniquePtr;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub struct CuDFColumnView {
    // Keep the column alive so views remain valid
    _column: Option<Arc<UniquePtr<libcudf_sys::ffi::Column>>>,
    // Keep the table alive so views remain valid
    _table: Option<Arc<crate::CuDFTable>>,
    inner: UniquePtr<libcudf_sys::ffi::ColumnView>,
    dt: DataType,
}

impl CuDFColumnView {
    /// Create a CuDFColumn from an existing cuDF column view
    pub fn new(view: UniquePtr<libcudf_sys::ffi::ColumnView>) -> Self {
        let cudf_dtype = view.data_type();
        let dt = cudf_type_to_arrow(cudf_dtype.id());
        let dt = dt.unwrap_or(DataType::Null);
        Self {
            _column: None,
            _table: None,
            inner: view,
            dt,
        }
    }

    pub fn from_column(col: UniquePtr<libcudf_sys::ffi::Column>) -> Self {
        let cudf_dtype = col.data_type();
        let dt = cudf_type_to_arrow(cudf_dtype.id());
        let dt = dt.unwrap_or(DataType::Null);
        let inner = col.view();
        Self {
            _column: Some(Arc::new(col)),
            _table: None,
            inner,
            dt,
        }
    }

    /// Create a CuDFColumn view from a table column, keeping the table alive
    pub fn from_table_column(
        table: Arc<crate::CuDFTable>,
        col_index: i32,
    ) -> Self {
        let view_ptr = table.view();
        let col_view = view_ptr.column(col_index);
        let cudf_dtype = col_view.inner.data_type();
        let dt = cudf_type_to_arrow(cudf_dtype.id());
        let dt = dt.unwrap_or(DataType::Null);
        Self {
            _column: None,
            _table: Some(table),
            inner: col_view.inner,
            dt,
        }
    }

    pub fn inner(&self) -> &UniquePtr<libcudf_sys::ffi::ColumnView> {
        &self.inner
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
    /// use libcudf_rs::CuDFColumnView;
    ///
    /// let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    /// let column = CuDFColumnView::from_arrow(&array)?;
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn from_arrow(array: &dyn Array) -> Result<Self, CuDFError> {
        if let Some(this) = array.as_any().downcast_ref::<Self>() {
            return Ok(this.clone());
        }
        let array_data = array.to_data();
        let ffi_array = FFI_ArrowArray::new(&array_data);
        let ffi_schema = FFI_ArrowSchema::try_from(array.data_type())?;

        let schema_ptr = &ffi_schema as *const FFI_ArrowSchema as *const u8;
        let array_ptr = &ffi_array as *const FFI_ArrowArray as *const u8;

        let column = unsafe { libcudf_sys::ffi::column_from_arrow(schema_ptr, array_ptr) }?;
        let view = column.view();
        let cudf_dtype = view.data_type();
        let Some(dt) = cudf_type_to_arrow(cudf_dtype.id()) else {
            return Err(ArrowError::NotYetImplemented(format!(
                "CuDF dtype {} does not map to any Arrow type",
                cudf_dtype.id()
            )))?;
        };

        Ok(Self {
            _column: Some(Arc::new(column)),
            _table: None,
            inner: view,
            dt,
        })
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
    /// use libcudf_rs::CuDFColumnView;
    ///
    /// let array = Int32Array::from(vec![1, 2, 3]);
    /// let column = CuDFColumnView::from_arrow(&array)?;
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
    /// use libcudf_rs::CuDFColumnView;
    ///
    /// let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    /// let column = CuDFColumnView::from_arrow(&array)?;
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
            _column: self._column.clone(),
            _table: self._table.clone(),
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
