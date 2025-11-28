//! Safe, idiomatic Rust bindings for cuDF
//!
//! This crate provides a safe wrapper around the cuDF C++ library,
//! enabling GPU-accelerated dataframe operations in Rust.
//!
//! # Examples
//!
//! ```no_run
//! use libcudf_rs::Table;
//!
//! // Read a Parquet file
//! let table = Table::from_parquet("data.parquet").expect("Failed to read Parquet");
//! println!("Loaded table with {} rows and {} columns",
//!          table.num_rows(), table.num_columns());
//!
//! // Write to Parquet
//! table.to_parquet("output.parquet").expect("Failed to write Parquet");
//! ```

use cxx::UniquePtr;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayData, StructArray};
use arrow::datatypes::Schema;
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;

pub use libcudf_sys::ffi;

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
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use libcudf_rs::Table;
    ///
    /// let table = Table::from_parquet("data.parquet")?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn from_parquet<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let path_str = path
            .as_ref()
            .to_str()
            .ok_or("Invalid path encoding")?;

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
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use libcudf_rs::Table;
    ///
    /// let table = Table::new();
    /// table.to_parquet("output.parquet")?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn to_parquet<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<dyn std::error::Error>> {
        let path_str = path
            .as_ref()
            .to_str()
            .ok_or("Invalid path encoding")?;

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
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn from_arrow(batch: RecordBatch) -> Result<Self, Box<dyn std::error::Error>> {
        // Convert RecordBatch to StructArray
        let struct_array = StructArray::from(batch.clone());
        let array_data: ArrayData = struct_array.into_data();

        // Export to Arrow C ABI
        let ffi_array = FFI_ArrowArray::new(&array_data);
        let ffi_schema = FFI_ArrowSchema::try_from(batch.schema().as_ref())?;

        // Get raw pointers - must keep ffi_schema and ffi_array alive during the FFI call
        let schema_ptr = &ffi_schema as *const FFI_ArrowSchema as *mut u8;
        let array_ptr = &ffi_array as *const FFI_ArrowArray as *mut u8;

        // Pass to cuDF via FFI (unsafe because we're passing raw pointers)
        let inner = unsafe { ffi::from_arrow(schema_ptr, array_ptr)? };
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
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn to_arrow(&self) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        // Create uninitialized Arrow C structures
        let mut ffi_schema = FFI_ArrowSchema::empty();
        let mut ffi_array = FFI_ArrowArray::empty();

        // Get raw pointers
        let schema_ptr = &mut ffi_schema as *mut FFI_ArrowSchema as *mut u8;
        let array_ptr = &mut ffi_array as *mut FFI_ArrowArray as *mut u8;

        // Get data from cuDF via FFI (unsafe because we're passing raw pointers)
        unsafe { ffi::to_arrow(&self.inner, schema_ptr, array_ptr)? };

        // Import schema and array from Arrow C ABI (unsafe because we're trusting the FFI data)
        let schema = Arc::new(Schema::try_from(&ffi_schema)?);
        let array_data = unsafe { from_ffi(ffi_array, &ffi_schema)? };
        let struct_array = StructArray::from(array_data);

        // Convert StructArray to RecordBatch
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

    /// Select specific columns from the table by their indices
    ///
    /// Returns a new table containing only the specified columns.
    ///
    /// # Arguments
    ///
    /// * `indices` - Slice of column indices to select
    ///
    /// # Errors
    ///
    /// Returns an error if any index is out of bounds
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use libcudf_rs::Table;
    ///
    /// let table = Table::from_parquet("data.parquet")?;
    /// // Select columns 0, 2, and 4
    /// let selected = table.select(&[0, 2, 4])?;
    /// assert_eq!(selected.num_columns(), 3);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn select(&self, indices: &[usize]) -> Result<Self, Box<dyn std::error::Error>> {
        let inner = ffi::select_columns(&self.inner, indices)?;
        Ok(Self { inner })
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
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn get_column(&self, index: usize) -> Result<Column, Box<dyn std::error::Error>> {
        let inner = ffi::get_column(&self.inner, index)?;
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
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn filter(&self, mask: &Column) -> Result<Self, Box<dyn std::error::Error>> {
        let inner = ffi::filter(&self.inner, &mask.inner)?;
        Ok(Self { inner })
    }
}

/// A GPU-accelerated column
///
/// This is a safe wrapper around cuDF's column type.
pub struct Column {
    inner: UniquePtr<ffi::Column>,
}

impl Column {
    /// Create a new boolean column from a slice of bool values
    ///
    /// # Arguments
    ///
    /// * `data` - Slice of boolean values
    ///
    /// # Examples
    ///
    /// ```
    /// use libcudf_rs::Column;
    ///
    /// let mask = Column::from_bools(&[true, false, true, true, false])?;
    /// assert_eq!(mask.size(), 5);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn from_bools(data: &[bool]) -> Result<Self, Box<dyn std::error::Error>> {
        let inner = ffi::create_boolean_column(data)?;
        Ok(Self { inner })
    }

    /// Get the number of elements in the column
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use libcudf_rs::Table;
    ///
    /// let table = Table::from_parquet("data.parquet")?;
    /// let column = table.get_column(0)?;
    /// println!("Column size: {}", column.size());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn size(&self) -> usize {
        self.inner.size()
    }
}

impl Default for Table {
    fn default() -> Self {
        Self::new()
    }
}

/// Get cuDF version information
///
/// # Examples
///
/// ```
/// use libcudf_rs::version;
///
/// println!("cuDF version: {}", version());
/// ```
pub fn version() -> String {
    ffi::get_cudf_version()
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

    #[test]
    fn test_version() {
        let ver = version();
        assert!(!ver.is_empty());
    }
}
