use crate::{CuDFColumnView, CuDFError, CuDFTable};
use cxx::UniquePtr;
use libcudf_sys::ffi;

/// A non-owning view of a GPU table
///
/// This is a safe wrapper around cuDF's table_view type.
/// Views provide a lightweight way to reference table data without ownership.
pub struct CuDFTableView {
    inner: UniquePtr<ffi::TableView>,
}

impl CuDFTableView {
    /// Create a new table view from a raw FFI table view
    pub(crate) fn new(inner: UniquePtr<ffi::TableView>) -> Self {
        Self { inner }
    }

    /// Create a table view from a slice of column view references
    ///
    /// # Arguments
    ///
    /// * `column_views` - A slice of column view references to combine into a table view
    ///
    /// # Errors
    ///
    /// Returns an error if the FFI call fails
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use arrow::array::{Int32Array, RecordBatch};
    /// use arrow::datatypes::{DataType, Field, Schema};
    /// use libcudf_rs::{CuDFColumnView, CuDFTableView};
    /// use std::sync::Arc;
    ///
    /// // Create column views
    /// let col1 = Int32Array::from(vec![1, 2, 3]);
    /// let col2 = Int32Array::from(vec![4, 5, 6]);
    /// let view1 = CuDFColumnView::from_arrow(&col1)?;
    /// let view2 = CuDFColumnView::from_arrow(&col2)?;
    ///
    /// // Create a table view from the column views
    /// let table_view = CuDFTableView::from_column_views(&[&view1, &view2])?;
    /// assert_eq!(table_view.num_columns(), 2);
    /// assert_eq!(table_view.num_rows(), 3);
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn from_column_views(column_views: &[&CuDFColumnView]) -> Result<Self, CuDFError> {
        let view_ptrs: Vec<*const ffi::ColumnView> = column_views
            .iter()
            .map(|view| view.inner().as_ref().unwrap() as *const ffi::ColumnView)
            .collect();

        let inner = ffi::create_table_view_from_column_views(&view_ptrs);
        Ok(Self { inner })
    }

    /// Filter the table using a boolean mask
    ///
    /// Returns a new table containing only the rows where the corresponding
    /// element in the boolean mask is `true`. This operation is stable: the
    /// input order is preserved in the output.
    ///
    /// # Arguments
    ///
    /// * `boolean_mask` - A boolean array where `true` indicates rows to keep
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The mask length does not match the table's number of rows
    /// - The mask is not a boolean type
    /// - There is insufficient GPU memory
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use arrow::array::{BooleanArray, Int32Array, RecordBatch};
    /// use arrow::datatypes::{DataType, Field, Schema};
    /// use libcudf_rs::{CuDFColumnView, CuDFTable};
    /// use std::sync::Arc;
    ///
    /// // Create a table
    /// let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
    /// let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    /// let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)])?;
    /// let table = CuDFTable::from_arrow_host(batch)?;
    ///
    /// // Create a boolean mask
    /// let mask = BooleanArray::from(vec![true, false, true, false, true]);
    /// let mask_column = CuDFColumnView::from_arrow(&mask)?;
    ///
    /// // Filter the table using a view
    /// let table_view = table.view();
    /// let filtered = table_view.apply_boolean_mask(&mask_column)?;
    /// assert_eq!(filtered.num_rows(), 3);
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn apply_boolean_mask(&self, boolean_mask: &CuDFColumnView) -> Result<CuDFTable, CuDFError> {
        let inner = ffi::apply_boolean_mask(&self.inner, boolean_mask.inner())?;
        Ok(CuDFTable::from_inner(inner))
    }

    /// Get the number of rows in the table view
    ///
    /// # Examples
    ///
    /// ```
    /// use libcudf_rs::CuDFTable;
    ///
    /// let table = CuDFTable::new();
    /// let view = table.view();
    /// assert_eq!(view.num_rows(), 0);
    /// ```
    pub fn num_rows(&self) -> usize {
        self.inner.num_rows()
    }

    /// Get the number of columns in the table view
    ///
    /// # Examples
    ///
    /// ```
    /// use libcudf_rs::CuDFTable;
    ///
    /// let table = CuDFTable::new();
    /// let view = table.view();
    /// assert_eq!(view.num_columns(), 0);
    /// ```
    pub fn num_columns(&self) -> usize {
        self.inner.num_columns()
    }

    /// Check if the table view is empty
    ///
    /// # Examples
    ///
    /// ```
    /// use libcudf_rs::CuDFTable;
    ///
    /// let table = CuDFTable::new();
    /// let view = table.view();
    /// assert!(view.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }

    /// Get a column view at the specified index
    ///
    /// # Arguments
    ///
    /// * `index` - The column index (0-based)
    pub fn column(&self, index: i32) -> CuDFColumnView {
        let inner = self.inner.column(index);
        CuDFColumnView::new(inner)
    }
}
