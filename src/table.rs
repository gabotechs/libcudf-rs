use cxx::UniquePtr;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayData, StructArray};
use arrow::datatypes::Schema;
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;

use crate::{ffi, Column, Result};

/// A GPU-accelerated table (similar to a DataFrame)
///
/// This is a safe wrapper around cuDF's table type.
pub struct Table {
    inner: UniquePtr<ffi::Table>,
}

impl Table {
    /// Create an empty table
    ///
    /// # Examples
    ///
    /// ```
    /// use libcudf_rs::Table;
    ///
    /// let table = Table::new();
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
    /// use libcudf_rs::Table;
    ///
    /// let table = Table::from_parquet("data.parquet")?;
    /// # Ok::<(), libcudf_rs::LibCuDFError>(())
    /// ```
    pub fn from_parquet<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_str = path
            .as_ref()
            .to_str()
            .ok_or_else(|| arrow::error::ArrowError::InvalidArgumentError(
                "Path contains invalid UTF-8".to_string()
            ))?;

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
    /// use libcudf_rs::Table;
    ///
    /// let table = Table::new();
    /// table.to_parquet("output.parquet")?;
    /// # Ok::<(), libcudf_rs::LibCuDFError>(())
    /// ```
    pub fn to_parquet<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let path_str = path
            .as_ref()
            .to_str()
            .ok_or_else(|| arrow::error::ArrowError::InvalidArgumentError(
                "Path contains invalid UTF-8".to_string()
            ))?;

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
    /// use libcudf_rs::Table;
    ///
    /// # let batch: RecordBatch = todo!();
    /// // Convert Arrow RecordBatch to cuDF table for GPU acceleration
    /// let table = Table::from_arrow(batch)?;
    /// # Ok::<(), libcudf_rs::LibCuDFError>(())
    /// ```
    pub fn from_arrow(batch: RecordBatch) -> Result<Self> {
        let struct_array = StructArray::from(batch.clone());
        let array_data: ArrayData = struct_array.into_data();

        let ffi_array = FFI_ArrowArray::new(&array_data);
        let ffi_schema = FFI_ArrowSchema::try_from(batch.schema().as_ref())?;

        // Create ArrowDeviceArray structure (describes CPU data location)
        #[repr(C)]
        struct ArrowDeviceArray {
            array: FFI_ArrowArray,
            device_id: i64,
            device_type: i32,
            sync_event: *mut std::ffi::c_void,
        }

        let device_array = ArrowDeviceArray {
            array: ffi_array,
            device_id: -1,  // CPU
            device_type: 1, // ARROW_DEVICE_CPU
            sync_event: std::ptr::null_mut(),
        };

        let schema_ptr = &ffi_schema as *const FFI_ArrowSchema as *mut u8;
        let device_array_ptr = &device_array as *const ArrowDeviceArray as *mut u8;

        let inner = unsafe { ffi::from_arrow_host(schema_ptr, device_array_ptr)? };
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
    /// use libcudf_rs::Table;
    ///
    /// let table = Table::from_parquet("data.parquet")?;
    /// // Perform GPU operations...
    ///
    /// // Convert back to Arrow for further processing
    /// let batch = table.to_arrow()?;
    /// # Ok::<(), libcudf_rs::LibCuDFError>(())
    /// ```
    pub fn to_arrow(&self) -> Result<RecordBatch> {
        #[repr(C)]
        struct ArrowDeviceArray {
            array: FFI_ArrowArray,
            device_id: i64,
            device_type: i32,
            sync_event: *mut std::ffi::c_void,
        }

        let mut ffi_schema = FFI_ArrowSchema::empty();
        let mut device_array = ArrowDeviceArray {
            array: FFI_ArrowArray::empty(),
            device_id: 0,
            device_type: 0,
            sync_event: std::ptr::null_mut(),
        };

        let schema_ptr = &mut ffi_schema as *mut FFI_ArrowSchema as *mut u8;
        let array_ptr = &mut device_array as *mut ArrowDeviceArray as *mut u8;

        unsafe {
            ffi::to_arrow_schema(&self.inner, schema_ptr)?;
            ffi::to_arrow_host_array(&self.inner, array_ptr)?;
        }

        let schema = Arc::new(Schema::try_from(&ffi_schema)?);
        let array_data = unsafe { from_ffi(device_array.array, &ffi_schema)? };
        let struct_array = StructArray::from(array_data);

        let batch = RecordBatch::try_new(schema, struct_array.columns().to_vec())?;

        Ok(batch)
    }

    /// Get the number of rows in the table
    ///
    /// # Examples
    ///
    /// ```
    /// use libcudf_rs::Table;
    ///
    /// let table = Table::new();
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
    /// use libcudf_rs::Table;
    ///
    /// let table = Table::new();
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
    /// use libcudf_rs::Table;
    ///
    /// let table = Table::new();
    /// assert!(table.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }

    /// Get a specific column from the table by index
    ///
    /// # Arguments
    ///
    /// * `index` - The column index (0-based)
    ///
    /// # Errors
    ///
    /// Returns an error if the index is out of bounds
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use libcudf_rs::Table;
    ///
    /// let table = Table::from_parquet("data.parquet")?;
    /// let column = table.get_column(0)?;
    /// println!("Column has {} elements", column.size());
    /// # Ok::<(), libcudf_rs::LibCuDFError>(())
    /// ```
    pub fn get_column(&self, index: usize) -> Result<Column> {
        if index >= self.num_columns() {
            return Err(arrow::error::ArrowError::InvalidArgumentError(
                format!("Column index {} out of bounds (table has {} columns)", index, self.num_columns())
            ).into());
        }
        let inner = ffi::table_get_column(&self.inner, index)?;
        Ok(Column { inner })
    }

    /// Filter rows based on a boolean mask column
    ///
    /// Returns a new table containing only the rows where the mask is true.
    ///
    /// # Arguments
    ///
    /// * `mask` - A boolean column where true means keep the row
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The mask has a different number of rows than the table
    /// - The mask is not a boolean column
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use libcudf_rs::Table;
    ///
    /// let table = Table::from_parquet("data.parquet")?;
    /// // Get a boolean column (e.g., from a comparison operation)
    /// let mask = table.get_column(0)?; // Assume this is a boolean column
    /// let filtered = table.filter(&mask)?;
    /// println!("Filtered to {} rows", filtered.num_rows());
    /// # Ok::<(), libcudf_rs::LibCuDFError>(())
    /// ```
    pub fn filter(&self, mask: &Column) -> Result<Self> {
        if mask.size() != self.num_rows() {
            return Err(arrow::error::ArrowError::InvalidArgumentError(
                format!("Boolean mask size {} must match table rows {}", mask.size(), self.num_rows())
            ).into());
        }
        let inner = ffi::apply_boolean_mask(&self.inner, &mask.inner)?;
        Ok(Self { inner })
    }
}

impl Default for Table {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_table() {
        let table = Table::new();
        assert_eq!(table.num_rows(), 0);
        assert_eq!(table.num_columns(), 0);
        assert!(table.is_empty());
    }

    #[test]
    fn test_default_table() {
        let table = Table::default();
        assert!(table.is_empty());
    }
}
