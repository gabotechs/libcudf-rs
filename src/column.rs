use crate::{cudf_type_to_arrow, CuDFError};
use arrow::array::{Array, ArrayData, ArrayRef};
use arrow::buffer::NullBuffer;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_schema::DataType;
use cxx::UniquePtr;
use std::any::Any;
use std::fmt::{Debug, Formatter};

pub struct CuDFColumn {
    inner: UniquePtr<libcudf_sys::ffi::Column>,
    dt: DataType,
}

impl CuDFColumn {
    /// Create a CuDFColumn from an existing cuDF column pointer
    pub fn new(inner: UniquePtr<libcudf_sys::ffi::Column>) -> Self {
        let cudf_dtype = inner.data_type();
        let dt = cudf_type_to_arrow(cudf_dtype.id());
        let dt = dt.unwrap_or(DataType::Null);
        Self { inner, dt }
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
    /// let column = CuDFColumn::from_arrow(&array)?;
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn from_arrow(array: &dyn Array) -> Result<Self, CuDFError> {
        let array_data = array.to_data();
        let ffi_array = FFI_ArrowArray::new(&array_data);
        let ffi_schema = FFI_ArrowSchema::try_from(array.data_type())?;

        let schema_ptr = &ffi_schema as *const FFI_ArrowSchema as *const u8;
        let array_ptr = &ffi_array as *const FFI_ArrowArray as *const u8;

        let inner = unsafe { libcudf_sys::ffi::column_from_arrow(schema_ptr, array_ptr) }?;

        Ok(Self::new(inner))
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
    /// use libcudf_rs::CuDFColumn;
    ///
    /// let array = Int32Array::from(vec![1, 2, 3]);
    /// let column = CuDFColumn::from_arrow(&array)?;
    /// let gpu_ptr = unsafe { column.data_ptr() };
    /// // Use CUDA APIs to work with gpu_ptr
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub unsafe fn data_ptr(&self) -> u64 {
        self.inner.view().data_ptr()
    }
}

impl Debug for CuDFColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl arrow::array::Array for CuDFColumn {
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

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
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
