use crate::cudf_reference::CuDFRef;
use crate::device_resource::resource_ref;
use crate::stream::{
    common_execution_stream, global_execution_stream, record_batch_execution_stream_from_columns,
    stream_ref,
};
use crate::{CuDFColumnView, CuDFError, CuDFStream};
use arrow::array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, StructArray};
use arrow::ffi::from_ffi;
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::{ArrowError, Schema, SchemaRef};
use cxx::UniquePtr;
use libcudf_sys::{ffi, ArrowDeviceArray};
use std::sync::Arc;

/// A non-owning view of a GPU table
///
/// This is a safe wrapper around cuDF's table_view type.
/// Views provide a lightweight way to reference table data without ownership.
pub struct CuDFTableView {
    inner: Arc<UniquePtr<ffi::TableView>>,
    // Keep cuDF owners alive until after the non-owning view is dropped.
    pub(crate) keepalive: Option<Arc<dyn CuDFRef>>,
    num_rows: usize,
    column_device_memory_sizes: Vec<usize>,
    stream: CuDFStream,
}

impl CuDFTableView {
    pub(crate) fn from_shared_view(
        inner: Arc<UniquePtr<ffi::TableView>>,
        keepalive: Option<Arc<dyn CuDFRef>>,
        num_rows: usize,
        column_device_memory_sizes: Vec<usize>,
        stream: CuDFStream,
    ) -> Self {
        Self {
            inner,
            keepalive,
            num_rows,
            column_device_memory_sizes,
            stream,
        }
    }

    pub(crate) fn inner(&self) -> &UniquePtr<ffi::TableView> {
        &self.inner
    }

    pub(crate) fn clone_inner(&self) -> Result<UniquePtr<ffi::TableView>, CuDFError> {
        let inner = self
            .inner
            .as_ref()
            .as_ref()
            .ok_or(CuDFError::NullHandle("table view"))?;
        Ok(ffi::table_view_clone(inner)?)
    }

    /// Return the CUDA stream on which this table becomes ready.
    pub fn execution_stream(&self) -> CuDFStream {
        self.stream.clone()
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
    /// let view1 = CuDFColumn::try_from_arrow_host(&col1)?.into_view();
    /// let view2 = CuDFColumn::try_from_arrow_host(&col2)?.into_view();
    ///
    /// // Create a table view from the column views
    /// let table_view = CuDFTableView::try_from_column_views(vec![view1, view2])?;
    /// assert_eq!(table_view.num_columns(), 2);
    /// assert_eq!(table_view.num_rows(), 3);
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn try_from_column_views(column_views: Vec<CuDFColumnView>) -> Result<Self, CuDFError> {
        let stream = common_execution_stream(
            column_views.iter().map(CuDFColumnView::execution_stream),
            "table-view columns",
        )?
        .unwrap_or(global_execution_stream()?);
        let mut view_ptrs: Vec<*const ffi::ColumnView> = Vec::with_capacity(column_views.len());
        let mut keepalives = Vec::with_capacity(column_views.len());
        let column_device_memory_sizes = column_views
            .iter()
            .map(CuDFColumnView::device_memory_size)
            .collect();
        for view in column_views {
            let inner = view
                .inner()
                .as_ref()
                .ok_or(CuDFError::NullHandle("column view"))?;
            view_ptrs.push(inner as _);
            keepalives.push(Arc::new(view) as Arc<dyn CuDFRef>)
        }

        let inner = unsafe { ffi::create_table_view_from_column_views(&view_ptrs) }?;
        let num_rows =
            crate::errors::cudf_size_to_usize(inner.num_rows()?, "table-view row count")?;
        Ok(Self {
            inner: Arc::new(inner),
            keepalive: Some(Arc::new(keepalives)),
            num_rows,
            column_device_memory_sizes,
            stream,
        })
    }

    /// Get the number of rows in the table view
    ///
    /// # Examples
    ///
    /// ```
    /// use libcudf_rs::CuDFTable;
    ///
    /// let table = CuDFTable::try_empty()?;
    /// let view = table.into_view();
    /// assert_eq!(view.num_rows(), 0);
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Get the number of columns in the table view
    ///
    /// # Examples
    ///
    /// ```
    /// use libcudf_rs::CuDFTable;
    ///
    /// let table = CuDFTable::try_empty()?;
    /// let view = table.into_view();
    /// assert_eq!(view.num_columns(), 0);
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn num_columns(&self) -> usize {
        self.column_device_memory_sizes.len()
    }

    /// Check if the table view is empty
    ///
    /// # Examples
    ///
    /// ```
    /// use libcudf_rs::CuDFTable;
    ///
    /// let table = CuDFTable::try_empty()?;
    /// let view = table.into_view();
    /// assert!(view.is_empty());
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }

    /// Get a column view at the specified index
    ///
    /// # Arguments
    ///
    /// * `index` - The column index (0-based)
    pub fn column(&self, index: i32) -> Result<CuDFColumnView, CuDFError> {
        let usize_index = usize::try_from(index).map_err(|_| {
            ArrowError::InvalidArgumentError(format!("negative column index {index}"))
        })?;
        if usize_index >= self.num_columns() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "column index {index} out of bounds for {} columns",
                self.num_columns()
            ))
            .into());
        }
        let inner = self.inner.column(index)?;
        CuDFColumnView::try_from_inner_with_device_size_on_stream(
            inner,
            Some(Arc::new(self.clone())),
            self.column_device_memory_sizes.get(usize_index).copied(),
            self.stream.clone(),
        )
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
    /// let table = CuDFTable::read_parquet("data.parquet")?;
    /// // Perform GPU operations...
    ///
    /// // Convert back to Arrow for further processing
    /// let batch = table.into_view().to_arrow_host()?;
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn to_arrow_host(&self) -> Result<RecordBatch, CuDFError> {
        let mut ffi_schema = FFI_ArrowSchema::empty();
        let mut device_array = ArrowDeviceArray::new_cpu();

        unsafe {
            let metadata = ffi::get_table_metadata(self.inner())?;
            ffi::to_arrow_schema(
                self.inner(),
                &metadata,
                &mut ffi_schema as *mut FFI_ArrowSchema as *mut u8,
            )?;
            let stream = self.stream.view()?;
            let mr = ffi::get_current_device_resource_ref();
            ffi::to_arrow_host_table(
                self.inner(),
                &mut device_array as *mut ArrowDeviceArray as *mut u8,
                stream_ref(&stream)?,
                resource_ref(&mr)?,
            )?;
            stream_ref(&stream)?.synchronize()?;
        }

        let schema = Arc::new(Schema::try_from(&ffi_schema)?);
        let array_data = unsafe { from_ffi(device_array.array, &ffi_schema)? };
        let struct_array = StructArray::from(array_data);

        // Carry the row count explicitly so zero-column batches (e.g. produced by
        // `FilterExec` with `projection=[]` for `COUNT(*) WHERE ...` plans) don't
        // trip Arrow's "must either specify a row count or at least one column" check.
        let options = RecordBatchOptions::new().with_row_count(Some(struct_array.len()));
        let batch =
            RecordBatch::try_new_with_options(schema, struct_array.columns().to_vec(), &options)?;

        Ok(batch)
    }

    /// Gets the Arrow Schema of the table view.
    pub fn schema(&self) -> Result<Schema, CuDFError> {
        // Extract schema information
        let mut ffi_schema = FFI_ArrowSchema::empty();
        unsafe {
            let metadata = ffi::get_table_metadata(self.inner())?;
            ffi::to_arrow_schema(
                self.inner(),
                &metadata,
                &mut ffi_schema as *mut FFI_ArrowSchema as *mut u8,
            )?;
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
            .map(|i| {
                self.column(i as i32)
                    .map(|column| Arc::new(column) as ArrayRef)
            })
            .collect::<Result<_, _>>()?;

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
            .map(|i| {
                self.column(i as i32)
                    .map(|column| Arc::new(column) as ArrayRef)
            })
            .collect::<Result<_, _>>()?;
        record_batch_with_schema(columns, schema, self.num_rows()).map_err(CuDFError::ArrowError)
    }

    /// Create a table view from a RecordBatch containing CuDF arrays (GPU)
    ///
    /// This expects the RecordBatch to already contain CuDF arrays allocated on GPU.
    /// The columns will be extracted and composed into a table view.
    pub fn try_from_record_batch(batch: &RecordBatch) -> Result<Self, CuDFError> {
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

        Self::try_from_column_views(column_views)
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
    record_batch_execution_stream_from_columns(&columns)
        .map_err(|error| ArrowError::InvalidArgumentError(error.to_string()))?;
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
            inner: Arc::clone(&self.inner),
            keepalive: self.keepalive.clone(),
            num_rows: self.num_rows,
            column_device_memory_sizes: self.column_device_memory_sizes.clone(),
            stream: self.stream.clone(),
        }
    }
}
