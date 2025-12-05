use crate::cudf_array::is_cudf_array;
use crate::table_view::CuDFTableView;
use crate::{CuDFColumnView, CuDFError};
use arrow::array::{Array, ArrayData, StructArray};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use arrow_schema::ArrowError;
use cxx::UniquePtr;
use libcudf_sys::{ffi, ArrowDeviceArray};
use std::path::Path;

/// A GPU-accelerated table (similar to a DataFrame)
///
/// This is a safe wrapper around cuDF's table type.
pub struct CuDFTable {
    inner: UniquePtr<ffi::Table>,
}

impl CuDFTable {
    /// Create a CuDFTable from a raw FFI table (internal use)
    pub(crate) fn from_inner(inner: UniquePtr<ffi::Table>) -> Self {
        Self { inner }
    }
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

    pub fn from_ptr(inner: UniquePtr<ffi::Table>) -> Self {
        Self { inner }
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

        let view = self.inner.view();
        ffi::write_parquet(&view, path_str)?;
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
    /// - The Arrow RecordBatch contains columns that are already in cuDF
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
    /// let table = CuDFTable::from_arrow_host(batch)?;
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn from_arrow_host(batch: RecordBatch) -> Result<Self, CuDFError> {
        for col in batch.columns() {
            if is_cudf_array(col) {
                return Err(ArrowError::InvalidArgumentError("Tried to move a RecordBatch from the host to CuDF, but a column was already in CuDF".to_string()))?;
            }
        }
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
        let inner = unsafe { ffi::table_from_arrow_host(schema_ptr, device_array_ptr) }?;

        Ok(Self { inner })
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

    /// Get a non-owning view of this table
    ///
    /// The returned view borrows from this table and remains valid as long as
    /// the table exists.
    pub fn view(&self) -> CuDFTableView {
        CuDFTableView {
            inner: self.inner.view(),
        }
    }

    /// Take ownership of the table's columns
    ///
    /// This consumes the table structure and returns its columns as a collection
    /// that can be individually released.
    pub fn take(&mut self) -> CuDFColumnCollection {
        CuDFColumnCollection::new(self.inner.pin_mut().take())
    }
}

/// A collection of cuDF columns
///
/// This type holds multiple columns and allows releasing them individually.
/// It's typically obtained by calling `take()` on a table.
pub struct CuDFColumnCollection {
    inner: UniquePtr<ffi::ColumnCollection>,
}

impl CuDFColumnCollection {
    /// Create a column collection from a cuDF ColumnCollection pointer
    pub fn new(inner: UniquePtr<ffi::ColumnCollection>) -> Self {
        Self { inner }
    }

    /// Get the number of columns in this collection
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Check if the collection is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Release ownership of the column at the specified index
    ///
    /// This removes the column from the collection and returns it.
    pub fn release(&mut self, index: usize) -> CuDFColumnView {
        CuDFColumnView::from_column(self.inner.pin_mut().release(index))
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
