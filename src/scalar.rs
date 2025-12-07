use crate::{cudf_type_to_arrow, CuDFColumn, CuDFColumnView, CuDFError};
use arrow::array::{Array, ArrayData, ArrayRef, Scalar};
use arrow::buffer::NullBuffer;
use arrow_schema::DataType;
use cxx::UniquePtr;
use std::any::Any;
use std::fmt::{Debug, Formatter};

/// A single value stored in GPU memory
///
/// CuDFScalar wraps a cuDF scalar and implements Arrow's Array trait with length 1.
/// This allows scalar values to be used in contexts that expect arrays, enabling
/// seamless integration with operations that work on both scalars and columns.
pub struct CuDFScalar {
    inner: UniquePtr<libcudf_sys::ffi::Scalar>,
    dt: DataType,
}

impl CuDFScalar {
    /// Create a CuDFScalar from an existing cuDF scalar
    pub fn new(inner: UniquePtr<libcudf_sys::ffi::Scalar>) -> Self {
        let cudf_dtype = inner.data_type();
        let dt = cudf_type_to_arrow(cudf_dtype.id());
        let dt = dt.unwrap_or(DataType::Null);
        Self { inner, dt }
    }

    /// Get a reference to the underlying cuDF scalar
    pub fn inner(&self) -> &UniquePtr<libcudf_sys::ffi::Scalar> {
        &self.inner
    }

    /// Convert an Arrow scalar to a cuDF scalar
    ///
    /// This creates a single-element column from the scalar, transfers it to GPU,
    /// and extracts the scalar from that column.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The Arrow scalar cannot be converted to cuDF format
    /// - There is insufficient GPU memory
    ///
    /// # Example
    ///
    /// ```no_run
    /// use arrow::array::{Int32Array, Scalar};
    /// use libcudf_rs::CuDFScalar;
    ///
    /// let array = Int32Array::from(vec![42]);
    /// let scalar = Scalar::new(&array);
    /// let cudf_scalar = CuDFScalar::from_arrow_host(scalar)?;
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn from_arrow_host<T: Array>(scalar: Scalar<T>) -> Result<Self, CuDFError> {
        // Convert scalar to a single-element array
        let array = scalar.into_inner();

        // Convert the array to a cuDF column (this copies to GPU)
        let column = CuDFColumn::from_arrow_host(&array)?.into_view();

        // Extract the scalar from the column at index 0
        let cudf_scalar = libcudf_sys::ffi::get_element(column.inner(), 0);

        Ok(Self::new(cudf_scalar))
    }
}

impl Debug for CuDFScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CuDFScalar: type={}", self.dt)
    }
}

impl Clone for CuDFScalar {
    fn clone(&self) -> Self {
        Self::new(self.inner.clone())
    }
}

impl Array for CuDFScalar {
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
        1
    }

    fn is_empty(&self) -> bool {
        false
    }

    fn offset(&self) -> usize {
        0
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
