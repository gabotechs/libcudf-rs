use cxx::UniquePtr;

use crate::ffi;

/// A GPU-accelerated column
///
/// This is a safe wrapper around cuDF's column type.
pub struct Column {
    pub(crate) inner: UniquePtr<ffi::Column>,
}

impl Column {
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
