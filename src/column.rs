use crate::cudf_type_to_arrow;
use arrow::array::{ArrayData, ArrayRef};
use arrow::buffer::NullBuffer;
use arrow_schema::DataType;
use cxx::UniquePtr;
use std::any::Any;
use std::fmt::{Debug, Formatter};

pub struct CuDFColumn {
    inner: UniquePtr<libcudf_sys::ffi::Column>,
    dt: DataType,
}

impl CuDFColumn {
    pub fn new(inner: UniquePtr<libcudf_sys::ffi::Column>) -> Self {
        let cudf_dtype = inner.data_type();
        let dt = cudf_type_to_arrow(cudf_dtype.id());
        let dt = dt.unwrap_or(DataType::Null);
        Self { inner, dt }
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
