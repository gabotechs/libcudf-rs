use cxx::UniquePtr;

use crate::{ffi, Result};

/// A GPU-accelerated column
///
/// This is a safe wrapper around cuDF's column type.
pub struct Column {
    pub(crate) inner: UniquePtr<ffi::Column>,
}

impl Column {
    /// Create a new boolean column from a slice of bool values
    ///
    /// # Arguments
    ///
    /// * `data` - Slice of boolean values
    ///
    /// # Errors
    ///
    /// Returns an error if there is insufficient GPU memory
    ///
    /// # Examples
    ///
    /// ```
    /// use libcudf_rs::Column;
    ///
    /// let mask = Column::from_bools(&[true, false, true, true, false])?;
    /// assert_eq!(mask.size(), 5);
    /// # Ok::<(), libcudf_rs::LibCuDFError>(())
    /// ```
    pub fn from_bools(data: &[bool]) -> Result<Self> {
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
    /// # Ok::<(), libcudf_rs::LibCuDFError>(())
    /// ```
    pub fn size(&self) -> usize {
        self.inner.size()
    }
}
