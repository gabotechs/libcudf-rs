use crate::cudf_reference::CuDFRef;
use crate::{CuDFColumnView, CuDFError};
use arrow::array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, StructArray};
use arrow::ffi::{from_ffi, FFI_ArrowArray};
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::{ArrowError, Schema, SchemaRef};
use cxx::UniquePtr;
use libcudf_sys::ffi;
use std::sync::Arc;

/// A non-owning view of a GPU table
///
/// This is a safe wrapper around cuDF's table_view type.
/// Views provide a lightweight way to reference table data without ownership.
pub struct CuDFTableView {
    // Keep the table alive so view remains valid
    pub(crate) _ref: Option<Arc<dyn CuDFRef>>,
    inner: UniquePtr<ffi::TableView>,
}

impl CuDFTableView {
    pub(crate) fn new_with_ref(
        inner: UniquePtr<ffi::TableView>,
        _ref: Option<Arc<dyn CuDFRef>>,
    ) -> Self {
        Self { _ref, inner }
    }

    pub fn inner(&self) -> &UniquePtr<ffi::TableView> {
        &self.inner
    }

    pub(crate) fn into_inner(self) -> UniquePtr<ffi::TableView> {
        self.inner
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
    /// use libcudf_rs::{CuDFColumn, CuDFTableView};
    /// use std::sync::Arc;
    ///
    /// // Create column views
    /// let col1 = Int32Array::from(vec![1, 2, 3]);
    /// let col2 = Int32Array::from(vec![4, 5, 6]);
    /// let view1 = CuDFColumn::from_arrow_host(&col1)?.into_view();
    /// let view2 = CuDFColumn::from_arrow_host(&col2)?.into_view();
    ///
    /// // Create a table view from the column views
    /// let table_view = CuDFTableView::from_column_views(vec![view1, view2])?;
    /// assert_eq!(table_view.num_columns(), 2);
    /// assert_eq!(table_view.num_rows(), 3);
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn from_column_views(column_views: Vec<CuDFColumnView>) -> Result<Self, CuDFError> {
        let mut view_ptrs: Vec<*const ffi::ColumnView> = Vec::with_capacity(column_views.len());
        let mut _refs = Vec::with_capacity(column_views.len());
        for view in column_views {
            view_ptrs.push(view.inner().as_ref().unwrap() as _);
            _refs.push(Arc::new(view) as Arc<dyn CuDFRef>)
        }

        let inner = ffi::create_table_view_from_column_views(&view_ptrs);
        Ok(Self {
            _ref: Some(Arc::new(_refs)),
            inner,
        })
    }

    /// Get the number of rows in the table view
    ///
    /// # Examples
    ///
    /// ```
    /// use libcudf_rs::CuDFTable;
    ///
    /// let table = CuDFTable::default();
    /// let view = table.into_view();
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
    /// let table = CuDFTable::default();
    /// let view = table.into_view();
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
    /// let table = CuDFTable::default();
    /// let view = table.into_view();
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
        CuDFColumnView::new_with_ref(inner, Some(Arc::new(self.clone())))
    }

    /// Convert the CuDF table allocated on the GPU to an Arrow RecordBatch allocated on the host.
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
    /// let batch = table.into_view().to_arrow_host()?;
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn to_arrow_host(&self) -> Result<RecordBatch, CuDFError> {
        let mut ffi_schema = FFI_ArrowSchema::empty();
        let mut ffi_array = FFI_ArrowArray::empty();

        unsafe {
            self.inner
                .to_arrow_schema(&mut ffi_schema as *mut FFI_ArrowSchema as *mut u8);
            self.inner
                .to_arrow_array(&mut ffi_array as *mut FFI_ArrowArray as *mut u8);
        }

        let schema = Arc::new(Schema::try_from(&ffi_schema)?);
        let array_data = unsafe { from_ffi(ffi_array, &ffi_schema)? };
        let struct_array = StructArray::from(array_data);

        // Carry the row count explicitly so zero-column batches (e.g. produced by
        // `FilterExec` with `projection=[]` for `COUNT(*) WHERE ...` plans) don't
        // trip Arrow's "must either specify a row count or at least one column" check.
        let options = RecordBatchOptions::new().with_row_count(Some(struct_array.len()));
        let batch = RecordBatch::try_new_with_options(
            schema,
            struct_array.columns().to_vec(),
            &options,
        )?;

        Ok(batch)
    }

    /// Gets the Arrow Schema of the table view.
    pub fn schema(&self) -> Result<Schema, CuDFError> {
        // Extract schema information
        let mut ffi_schema = FFI_ArrowSchema::empty();
        unsafe {
            self.inner
                .to_arrow_schema(&mut ffi_schema as *mut FFI_ArrowSchema as *mut u8);
        }
        Ok(Schema::try_from(&ffi_schema)?)
    }

    /// Create a RecordBatch from the table view, keeping data on GPU
    ///
    /// This creates a RecordBatch where each column is a CuDFColumnView (GPU array).
    /// Unlike `to_arrow_host()`, this does NOT copy data to host memory.
    pub fn to_record_batch(&self) -> Result<RecordBatch, CuDFError> {
        // Create CuDFColumnView for each column (keeps data on GPU)
        let columns: Vec<ArrayRef> = (0..self.num_columns())
            .map(|i| Arc::new(self.column(i as i32)) as _)
            .collect();

        let options = RecordBatchOptions::new().with_row_count(Some(self.num_rows()));
        Ok(RecordBatch::try_new_with_options(
            Arc::new(self.schema()?),
            columns,
            &options,
        )?)
    }

    /// Wrap GPU columns in a `RecordBatch` whose types match `schema`.
    ///
    /// Delegates to [`record_batch_with_schema`]. Use this when the columns
    /// are still in a `CuDFTableView`.
    pub fn to_record_batch_with_schema(
        &self,
        schema: &SchemaRef,
    ) -> Result<RecordBatch, CuDFError> {
        if self.num_columns() != schema.fields().len() {
            return Err(CuDFError::ArrowError(ArrowError::InvalidArgumentError(
                format!(
                    "to_record_batch_with_schema: table has {} columns but schema has {} fields",
                    self.num_columns(),
                    schema.fields().len()
                ),
            )));
        }
        let columns: Vec<ArrayRef> = (0..self.num_columns())
            .map(|i| Arc::new(self.column(i as i32)) as _)
            .collect();
        record_batch_with_schema(columns, schema, self.num_rows()).map_err(CuDFError::ArrowError)
    }

    /// Create a table view from a RecordBatch containing CuDF arrays (GPU)
    ///
    /// This expects the RecordBatch to already contain CuDF arrays allocated on GPU.
    /// The columns will be extracted and composed into a table view.
    pub fn from_record_batch(batch: &RecordBatch) -> Result<Self, CuDFError> {
        let column_views: Result<Vec<_>, _> = batch
            .columns()
            .iter()
            .map(|col| {
                let Some(col) = col.as_any().downcast_ref::<CuDFColumnView>() else {
                    return Err(CuDFError::ArrowError(ArrowError::InvalidArgumentError(
                        "Expected all Arrays in RecordBatch to be CuDFColumnView".to_string(),
                    )));
                };
                Ok(col.clone())
            })
            .collect();
        let column_views = column_views?;

        Self::from_column_views(column_views)
    }
}

/// Build a `RecordBatch`, relabeling any `CuDFColumnView` whose type differs from
/// the corresponding `schema` field.
///
/// cuDF normalises decimal precision to the storage maximum (e.g. 38 for Decimal128).
/// This function restores the declared precision so `RecordBatch::try_new` accepts it.
/// All GPU `RecordBatch` creation should go through this function instead of calling
/// `RecordBatch::try_new` directly.
///
/// `num_rows` is required so zero-column batches (e.g. produced by `FilterExec`
/// with `projection=[]` for `COUNT(*) WHERE ...` plans) carry their row count.
pub fn record_batch_with_schema(
    columns: Vec<ArrayRef>,
    schema: &SchemaRef,
    num_rows: usize,
) -> Result<RecordBatch, ArrowError> {
    let relabeled: Vec<ArrayRef> = columns
        .into_iter()
        .zip(schema.fields())
        .map(|(col, field)| {
            if col.data_type() != field.data_type() {
                if let Some(v) = col.as_any().downcast_ref::<CuDFColumnView>() {
                    return Arc::new(v.clone().with_data_type(field.data_type().clone())) as _;
                }
            }
            col
        })
        .collect();
    let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
    RecordBatch::try_new_with_options(Arc::clone(schema), relabeled, &options)
}

impl Clone for CuDFTableView {
    fn clone(&self) -> Self {
        Self {
            _ref: self._ref.clone(),
            inner: self.inner.clone(),
        }
    }
}
