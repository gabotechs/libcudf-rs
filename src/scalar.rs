use crate::cudf_type_to_arrow;
use arrow::array::{Array, ArrayData, ArrayRef, Scalar};
use arrow::buffer::NullBuffer;
use arrow_schema::DataType;
use cxx::UniquePtr;
use std::any::Any;
use std::fmt::{Debug, Formatter};

pub struct CuDFScalar {
    inner: UniquePtr<libcudf_sys::ffi::Scalar>,
    dt: DataType,
}

impl CuDFScalar {
    pub fn new(inner: UniquePtr<libcudf_sys::ffi::Scalar>) -> Self {
        let cudf_dtype = inner.data_type();
        let dt = cudf_type_to_arrow(cudf_dtype.id());
        let dt = dt.unwrap_or(DataType::Null);
        Self { inner, dt }
    }

    pub fn inner(&self) -> &UniquePtr<libcudf_sys::ffi::Scalar> {
        &self.inner
    }

    pub fn from_arrow_host<T: Array>(scalar: Scalar<T>) -> Self {
        todo!()
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

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
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
