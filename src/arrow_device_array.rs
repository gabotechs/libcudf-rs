use crate::CuDFError;
use arrow::array::{Array, ArrayData, StructArray};
use arrow::ffi::FFI_ArrowArray;
use arrow::record_batch::RecordBatch;
use arrow_schema::ffi::FFI_ArrowSchema;

pub enum CuDFDeviceType {
    Cpu,
    Cuda(usize),
}

pub struct CuDFArrowDeviceArray {
    batch: RecordBatch,
    device: CuDFDeviceType,
}

impl CuDFArrowDeviceArray {
    pub fn new_cuda(batch: RecordBatch) -> Self {
        CuDFArrowDeviceArray {
            batch,
            device: CuDFDeviceType::Cuda(0),
        }
    }

    pub fn new_cuda_device(batch: RecordBatch, id: usize) -> Self {
        CuDFArrowDeviceArray {
            batch,
            device: CuDFDeviceType::Cuda(id),
        }
    }

    pub fn new_cpu(batch: RecordBatch) -> Self {
        CuDFArrowDeviceArray {
            batch,
            device: CuDFDeviceType::Cpu,
        }
    }

    pub fn schema(&self) -> Result<FFI_ArrowSchema, CuDFError> {
        let schema = self.batch.schema().as_ref().clone();
        let ffi_schema = FFI_ArrowSchema::try_from(schema)?;
        Ok(ffi_schema)
    }
}

impl TryFrom<CuDFArrowDeviceArray> for libcudf_sys::ArrowDeviceArray {
    type Error = CuDFError;

    fn try_from(value: CuDFArrowDeviceArray) -> Result<Self, Self::Error> {
        let struct_array = StructArray::from(value.batch);
        let array_data: ArrayData = struct_array.into_data();

        let ffi_array = FFI_ArrowArray::new(&array_data);

        Ok(libcudf_sys::ArrowDeviceArray {
            array: ffi_array,
            device_id: match value.device {
                CuDFDeviceType::Cpu => -1,
                CuDFDeviceType::Cuda(n) => n as i64,
            },
            device_type: match value.device {
                CuDFDeviceType::Cpu => 1,
                CuDFDeviceType::Cuda(_) => 2,
            },
            sync_event: std::ptr::null_mut(),
        })
    }
}
