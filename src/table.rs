use crate::CuDFError;
use arrow::array::{Array, ArrayData, StructArray};
use arrow::datatypes::Schema;
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use cxx::UniquePtr;
use libcudf_sys::{ffi, ArrowDeviceArray};
use std::path::Path;
use std::sync::Arc;

/// A GPU-accelerated table (similar to a DataFrame)
///
/// This is a safe wrapper around cuDF's table type.
pub struct CuDFTable {
    inner: UniquePtr<ffi::Table>,
}

impl CuDFTable {
    /// Create an empty table
    ///
    /// # Examples
    ///
    /// ```
    /// use libcudf_rs::CuDFTable;
    ///
    /// let table = CuDFTable::new();
    /// assert_eq!(table.num_rows(), 0);
    /// assert_eq!(table.num_columns(), 0);
    /// ```
    pub fn new() -> Self {
        Self {
            inner: ffi::create_empty_table(),
        }
    }

    /// Read a table from a Parquet file
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the Parquet file
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The file does not exist or cannot be read
    /// - The Parquet format is invalid
    /// - There is insufficient GPU memory
    /// - The path contains invalid UTF-8
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use libcudf_rs::CuDFTable;
    ///
    /// let table = CuDFTable::from_parquet("data.parquet")?;
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn from_parquet<P: AsRef<Path>>(path: P) -> Result<Self, CuDFError> {
        let path_str = path.as_ref().to_str().ok_or_else(|| {
            arrow::error::ArrowError::InvalidArgumentError(
                "Path contains invalid UTF-8".to_string(),
            )
        })?;

        let inner = ffi::read_parquet(path_str)?;
        Ok(Self { inner })
    }

    /// Write the table to a Parquet file
    ///
    /// # Arguments
    ///
    /// * `path` - Path where the Parquet file will be written
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The file cannot be created or written
    /// - There is insufficient GPU memory
    /// - The path contains invalid UTF-8
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use libcudf_rs::CuDFTable;
    ///
    /// let table = CuDFTable::new();
    /// table.to_parquet("output.parquet")?;
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn to_parquet<P: AsRef<Path>>(&self, path: P) -> Result<(), CuDFError> {
        let path_str = path.as_ref().to_str().ok_or_else(|| {
            arrow::error::ArrowError::InvalidArgumentError(
                "Path contains invalid UTF-8".to_string(),
            )
        })?;

        ffi::write_parquet(&self.inner, path_str)?;
        Ok(())
    }

    /// Create a table from an Arrow RecordBatch
    ///
    /// This enables seamless integration with arrow-rs, allowing you to use
    /// Arrow's rich ecosystem and then accelerate operations with cuDF on GPU.
    ///
    /// # Arguments
    ///
    /// * `batch` - An Arrow RecordBatch
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The Arrow data cannot be converted to cuDF format
    /// - There is insufficient GPU memory
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use arrow::record_batch::RecordBatch;
    /// use libcudf_rs::CuDFTable;
    ///
    /// # let batch: RecordBatch = todo!();
    /// // Convert Arrow RecordBatch to cuDF table for GPU acceleration
    /// let table = CuDFTable::from_arrow(batch)?;
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn from_arrow(batch: RecordBatch) -> Result<Self, CuDFError> {
        let schema = batch.schema().as_ref().clone();
        let struct_array = StructArray::from(batch);
        let array_data: ArrayData = struct_array.into_data();

        let ffi_array = FFI_ArrowArray::new(&array_data);
        let ffi_schema = FFI_ArrowSchema::try_from(schema)?;

        let device_array = ArrowDeviceArray {
            array: ffi_array,
            device_id: -1,
            device_type: 1, // CPU
            sync_event: std::ptr::null_mut(),
            reserved: [0; 3],
        };

        let schema_ptr = &ffi_schema as *const FFI_ArrowSchema as *const u8;
        let device_array_ptr = &device_array as *const ArrowDeviceArray as *const u8;
        let inner = unsafe { ffi::from_arrow_host(schema_ptr, device_array_ptr) }?;

        Ok(Self { inner })
    }

    /// Convert the table to an Arrow RecordBatch
    ///
    /// This allows you to use cuDF for GPU-accelerated operations and then
    /// return the results to arrow-rs for further processing or output.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The cuDF data cannot be converted to Arrow format
    /// - There is insufficient memory
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use libcudf_rs::CuDFTable;
    ///
    /// let table = CuDFTable::from_parquet("data.parquet")?;
    /// // Perform GPU operations...
    ///
    /// // Convert back to Arrow for further processing
    /// let batch = table.to_arrow()?;
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn to_arrow(&self) -> Result<RecordBatch, CuDFError> {
        let mut ffi_schema = FFI_ArrowSchema::empty();
        let mut ffi_array = FFI_ArrowArray::empty();

        unsafe {
            let view = self.inner.view();
            view.to_arrow_schema(&mut ffi_schema as *mut FFI_ArrowSchema as *mut u8)?;
            view.to_arrow_array(&mut ffi_array as *mut FFI_ArrowArray as *mut u8)?;
        }

        let schema = Arc::new(Schema::try_from(&ffi_schema)?);
        let array_data = unsafe { from_ffi(ffi_array, &ffi_schema)? };
        let struct_array = StructArray::from(array_data);

        let batch = RecordBatch::try_new(schema, struct_array.columns().to_vec())?;

        Ok(batch)
    }

    /// Get the number of rows in the table
    ///
    /// # Examples
    ///
    /// ```
    /// use libcudf_rs::CuDFTable;
    ///
    /// let table = CuDFTable::new();
    /// assert_eq!(table.num_rows(), 0);
    /// ```
    pub fn num_rows(&self) -> usize {
        self.inner.num_rows()
    }

    /// Get the number of columns in the table
    ///
    /// # Examples
    ///
    /// ```
    /// use libcudf_rs::CuDFTable;
    ///
    /// let table = CuDFTable::new();
    /// assert_eq!(table.num_columns(), 0);
    /// ```
    pub fn num_columns(&self) -> usize {
        self.inner.num_columns()
    }

    /// Check if the table is empty
    ///
    /// # Examples
    ///
    /// ```
    /// use libcudf_rs::CuDFTable;
    ///
    /// let table = CuDFTable::new();
    /// assert!(table.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }
}

impl Default for CuDFTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_table() {
        let table = CuDFTable::new();
        assert_eq!(table.num_rows(), 0);
        assert_eq!(table.num_columns(), 0);
        assert!(table.is_empty());
    }

    #[test]
    fn test_default_table() {
        let table = CuDFTable::default();
        assert!(table.is_empty());
    }
}
