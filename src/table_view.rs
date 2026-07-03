use crate::deferred_operation::deferred;
use crate::execution_policy;
use crate::sort;
use crate::stream_readiness::{CuDFStreamReady, CuDFTableReadiness};
use crate::{
    CuDFColumn, CuDFColumnView, CuDFError, CuDFOperation, CuDFTable, CuDFViewStorage, SortOrder,
};
use arrow_schema::ArrowError;
use cxx::UniquePtr;
use libcudf_sys::ffi;
use std::sync::Arc;

/// A non-owning view of a GPU table
///
/// This is a safe wrapper around cuDF's table_view type.
/// Views provide a lightweight way to reference table data without ownership.
pub struct CuDFTableView {
    // Keep backing storage alive so the view remains valid.
    storage: Option<CuDFViewStorage>,
    inner: UniquePtr<ffi::TableView>,
    stream_readiness: CuDFTableReadiness,
}

impl CuDFTableView {
    pub(crate) fn from_view(
        inner: UniquePtr<ffi::TableView>,
        storage: Option<CuDFViewStorage>,
        stream_readiness: CuDFTableReadiness,
    ) -> Self {
        Self {
            storage,
            inner,
            stream_readiness,
        }
    }

    pub(crate) fn inner(&self) -> &UniquePtr<ffi::TableView> {
        &self.inner
    }

    pub(crate) fn into_inner(self) -> UniquePtr<ffi::TableView> {
        self.inner
    }

    /// Create a table view from column views.
    ///
    /// The returned table view keeps each input column view alive and records
    /// each column's stream readiness independently.
    ///
    /// # Arguments
    ///
    /// * `column_views` - Column views to combine into a table view.
    ///
    /// # Errors
    ///
    /// Returns an error if any input column view has a null cuDF handle.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use arrow::array::{Int32Array, RecordBatch};
    /// use arrow::datatypes::{DataType, Field, Schema};
    /// use libcudf_rs::{CuDFColumn, CuDFExecutionContext, CuDFTableView};
    /// use std::sync::Arc;
    ///
    /// let col1 = Int32Array::from(vec![1, 2, 3]);
    /// let col2 = Int32Array::from(vec![4, 5, 6]);
    /// let ctx = CuDFExecutionContext::try_new_non_blocking()?;
    /// let view1 = ctx.execute(CuDFColumn::from_arrow_host(&col1))?.into_view();
    /// let view2 = ctx.execute(CuDFColumn::from_arrow_host(&col2))?.into_view();
    ///
    /// let table_view = CuDFTableView::from_column_views(vec![view1, view2])?;
    /// assert_eq!(table_view.num_columns(), 2);
    /// assert_eq!(table_view.num_rows(), 3);
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn from_column_views(column_views: Vec<CuDFColumnView>) -> Result<Self, CuDFError> {
        let mut view_ptrs: Vec<*const ffi::ColumnView> = Vec::with_capacity(column_views.len());
        let mut storage = Vec::with_capacity(column_views.len());
        let dependencies = column_views
            .iter()
            .map(|view| view.stream_readiness().cloned())
            .collect::<Vec<_>>();
        for view in column_views {
            let inner = view
                .inner()
                .as_ref()
                .ok_or(CuDFError::NullHandle("column view"))?;
            view_ptrs.push(inner as _);
            let keepalive: CuDFViewStorage = Arc::new(view);
            storage.push(keepalive)
        }

        let inner = ffi::create_table_view_from_column_views(&view_ptrs);
        let num_columns = inner.num_columns();
        let storage: CuDFViewStorage = Arc::new(storage);
        Ok(Self {
            storage: Some(storage),
            inner,
            stream_readiness: CuDFTableReadiness::columns(dependencies, num_columns),
        })
    }

    /// Get the number of rows in the table view
    ///
    /// # Examples
    ///
    /// ```no_run
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
    /// ```no_run
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
    /// ```no_run
    /// use libcudf_rs::CuDFTable;
    ///
    /// let table = CuDFTable::default();
    /// let view = table.into_view();
    /// assert!(view.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }

    /// Get a column view by index.
    ///
    /// The returned view keeps this table view alive and preserves the selected
    /// column's stream readiness metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if `index` is out of bounds or cannot fit in cuDF's
    /// column index type.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use arrow::array::{Array, Int32Array, RecordBatch};
    /// use arrow_schema::{DataType, Field, Schema};
    /// use libcudf_rs::{CuDFExecutionContext, CuDFTable};
    /// use std::sync::Arc;
    ///
    /// let schema = Schema::new(vec![
    ///     Field::new("a", DataType::Int32, false),
    ///     Field::new("b", DataType::Int32, false),
    /// ]);
    /// let batch = RecordBatch::try_new(
    ///     Arc::new(schema),
    ///     vec![
    ///         Arc::new(Int32Array::from(vec![1, 2, 3])),
    ///         Arc::new(Int32Array::from(vec![4, 5, 6])),
    ///     ],
    /// )?;
    ///
    /// let ctx = CuDFExecutionContext::try_new_non_blocking()?;
    /// let table = ctx.execute(CuDFTable::from_arrow_host(batch))?.into_view();
    /// let column = table.column(1)?;
    ///
    /// assert_eq!(column.len(), 3);
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn column(&self, index: usize) -> Result<CuDFColumnView, CuDFError> {
        let column_index = self.checked_column_index(index)?;
        let inner = self.inner.column(column_index);
        let storage: CuDFViewStorage = Arc::new(self.clone());
        Ok(CuDFColumnView::from_view(
            inner,
            Some(storage),
            self.stream_readiness.column(index),
        ))
    }

    /// Build a table view from selected columns.
    ///
    /// Column order follows `indices`, and each selected column keeps its
    /// source lifetime and stream readiness metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if any selected column index is out of bounds or cannot
    /// fit in cuDF's column index type.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use arrow::array::{Int32Array, RecordBatch};
    /// use arrow_schema::{DataType, Field, Schema};
    /// use libcudf_rs::{CuDFExecutionContext, CuDFTable};
    /// use std::sync::Arc;
    ///
    /// let schema = Schema::new(vec![
    ///     Field::new("a", DataType::Int32, false),
    ///     Field::new("b", DataType::Int32, false),
    /// ]);
    /// let batch = RecordBatch::try_new(
    ///     Arc::new(schema),
    ///     vec![
    ///         Arc::new(Int32Array::from(vec![1, 2, 3])),
    ///         Arc::new(Int32Array::from(vec![4, 5, 6])),
    ///     ],
    /// )?;
    ///
    /// let ctx = CuDFExecutionContext::try_new_non_blocking()?;
    /// let table = ctx.execute(CuDFTable::from_arrow_host(batch))?.into_view();
    /// let selected = table.select_columns(&[1, 0])?;
    ///
    /// assert_eq!(selected.num_columns(), 2);
    /// assert_eq!(selected.num_rows(), 3);
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn select_columns(&self, indices: &[usize]) -> Result<Self, CuDFError> {
        let column_indices = indices
            .iter()
            .map(|&index| self.checked_column_index(index))
            .collect::<Result<Vec<_>, _>>()?;
        let dependencies = indices
            .iter()
            .map(|&index| self.stream_readiness.column(index))
            .collect::<Vec<_>>();
        let storage: CuDFViewStorage = Arc::new(self.clone());
        Ok(Self::from_view(
            self.inner.select(&column_indices),
            Some(storage),
            CuDFTableReadiness::columns(dependencies, indices.len()),
        ))
    }

    fn checked_column_index(&self, index: usize) -> Result<i32, CuDFError> {
        if index >= self.num_columns() {
            return Err(CuDFError::ArrowError(ArrowError::InvalidArgumentError(
                format!(
                    "column index {index} out of bounds for table with {} columns",
                    self.num_columns()
                ),
            )));
        }
        i32::try_from(index).map_err(|_| {
            CuDFError::ArrowError(ArrowError::InvalidArgumentError(format!(
                "column index {index} exceeds cuDF's i32 column index range"
            )))
        })
    }

    /// Gather rows from this table using a gather-map column.
    ///
    /// The gather map contains row indices into this table. Executing the
    /// returned operation produces a new [`CuDFTable`] whose rows follow the
    /// order described by `gather_map`. This is commonly used after
    /// [`stable_sorted_order`](Self::stable_sorted_order) when applying a
    /// computed row ordering.
    ///
    /// # Errors
    ///
    /// Execution returns an error if:
    /// - `gather_map` contains an out-of-bounds row index
    /// - `gather_map` has an unsupported type
    /// - cuDF cannot allocate the output table
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use libcudf_rs::{CuDFExecutionContext, CuDFTable, SortOrder};
    ///
    /// let ctx = CuDFExecutionContext::try_new_non_blocking()?;
    /// let table = ctx.execute(CuDFTable::read_parquet("data.parquet"))?;
    /// let view = table.into_view();
    ///
    /// let sort_orders = [SortOrder::AscendingNullsLast];
    /// let indices = ctx.execute(view.stable_sorted_order(&sort_orders))?;
    /// let indices = std::sync::Arc::new(indices).view();
    ///
    /// let sorted = ctx.execute(view.gather(&indices))?;
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn gather<'a>(
        &'a self,
        gather_map: &'a CuDFColumnView,
    ) -> impl CuDFOperation<Output = CuDFTable> + 'a {
        deferred(move |ctx| {
            let mut launch = execution_policy::launch(ctx)?;
            launch.wait_table(self)?;
            launch.wait_column(gather_map)?;
            let inner = ffi::gather(
                self.inner(),
                gather_map.inner(),
                launch.stream()?,
                launch.resource(),
            )?;
            launch.ready_table(CuDFTable::from_inner(inner))
        })
    }

    /// Filter this table using a boolean mask column.
    ///
    /// The output contains only rows whose corresponding mask value is `true`.
    /// cuDF preserves the input row order for rows that pass the filter. The
    /// operation waits for both this table and `boolean_mask` to be ready on the
    /// execution context stream.
    ///
    /// # Errors
    ///
    /// Execution returns an error if:
    /// - `boolean_mask` is not a boolean column
    /// - `boolean_mask.len()` does not match this table's row count
    /// - cuDF cannot allocate the output table
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use arrow::array::{BooleanArray, Int32Array, RecordBatch};
    /// use arrow_schema::{DataType, Field, Schema};
    /// use libcudf_rs::{CuDFColumn, CuDFExecutionContext, CuDFTable};
    /// use std::sync::Arc;
    ///
    /// let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
    /// let values = Int32Array::from(vec![1, 2, 3, 4, 5]);
    /// let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(values)])?;
    ///
    /// let ctx = CuDFExecutionContext::try_new_non_blocking()?;
    /// let table = ctx.execute(CuDFTable::from_arrow_host(batch))?.into_view();
    /// let mask = BooleanArray::from(vec![true, false, true, false, true]);
    /// let mask = ctx.execute(CuDFColumn::from_arrow_host(&mask))?.into_view();
    ///
    /// let filtered = ctx.execute(table.filter(&mask))?;
    /// assert_eq!(filtered.num_rows(), 3);
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn filter<'a>(
        &'a self,
        boolean_mask: &'a CuDFColumnView,
    ) -> impl CuDFOperation<Output = CuDFTable> + 'a {
        deferred(move |ctx| {
            let mut launch = execution_policy::launch(ctx)?;
            launch.wait_table(self)?;
            launch.wait_column(boolean_mask)?;
            let inner = ffi::apply_boolean_mask(
                self.inner(),
                boolean_mask.inner(),
                launch.stream()?,
                launch.resource(),
            )?;
            launch.ready_table(CuDFTable::from_inner(inner))
        })
    }

    /// Stable-sort this table by selected key columns.
    ///
    /// `key_columns` identifies the columns to sort by, in precedence order.
    /// `sort_orders` may be empty to use cuDF defaults, or it must contain one
    /// [`SortOrder`] per key column. The output table contains all input
    /// columns, reordered by the selected keys.
    ///
    /// # Errors
    ///
    /// Execution returns an error if:
    /// - `key_columns` is empty
    /// - `key_columns.len()` does not match `sort_orders.len()`
    /// - any key column index is out of bounds
    /// - cuDF cannot allocate the output table
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use libcudf_rs::{CuDFExecutionContext, CuDFTable, SortOrder};
    ///
    /// let ctx = CuDFExecutionContext::try_new_non_blocking()?;
    /// let table = ctx.execute(CuDFTable::read_parquet("data.parquet"))?;
    /// let view = table.into_view();
    ///
    /// let sorted = ctx.execute(view.sort_by(
    ///     &[0, 2],
    ///     &[SortOrder::AscendingNullsLast, SortOrder::DescendingNullsFirst],
    /// ))?;
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn sort_by<'a>(
        &'a self,
        key_columns: &'a [usize],
        sort_orders: &'a [SortOrder],
    ) -> impl CuDFOperation<Output = CuDFTable> + 'a {
        deferred(move |ctx| sort::sort_by_on_context(ctx, self, key_columns, sort_orders))
    }

    /// Stable-sort this table lexicographically by all columns.
    ///
    /// `sort_orders` may be empty to use cuDF defaults, or it must contain one
    /// [`SortOrder`] for every column in this table. Column order in the table
    /// determines sort precedence.
    ///
    /// # Errors
    ///
    /// Execution returns an error if `sort_orders` does not match the table
    /// shape or cuDF cannot allocate the output table.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use libcudf_rs::{CuDFExecutionContext, CuDFTable, SortOrder};
    ///
    /// let ctx = CuDFExecutionContext::try_new_non_blocking()?;
    /// let table = ctx.execute(CuDFTable::read_parquet("data.parquet"))?;
    /// let view = table.into_view();
    ///
    /// let orders = [SortOrder::AscendingNullsLast, SortOrder::DescendingNullsFirst];
    /// let sorted = ctx.execute(view.sort_by_all(&orders))?;
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn sort_by_all<'a>(
        &'a self,
        sort_orders: &'a [SortOrder],
    ) -> impl CuDFOperation<Output = CuDFTable> + 'a {
        deferred(move |ctx| sort::sort_by_all_on_context(ctx, self, sort_orders))
    }

    /// Compute the stable row order that would sort this table.
    ///
    /// Executing the returned operation produces a column of row indices rather
    /// than reordering the table. This is useful for Top-K workflows or for
    /// applying the same order to another table with [`gather`](Self::gather).
    /// `sort_orders` may be empty to use cuDF defaults, or it must contain one
    /// [`SortOrder`] per column in this table.
    ///
    /// # Errors
    ///
    /// Execution returns an error if `sort_orders` does not match the table
    /// shape or cuDF cannot allocate the output index column.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use libcudf_rs::{CuDFExecutionContext, CuDFTable, SortOrder};
    ///
    /// let ctx = CuDFExecutionContext::try_new_non_blocking()?;
    /// let table = ctx.execute(CuDFTable::read_parquet("data.parquet"))?;
    /// let view = table.into_view();
    ///
    /// let orders = [SortOrder::AscendingNullsLast, SortOrder::DescendingNullsFirst];
    /// let indices = ctx.execute(view.stable_sorted_order(&orders))?;
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn stable_sorted_order<'a>(
        &'a self,
        sort_orders: &'a [SortOrder],
    ) -> impl CuDFOperation<Output = CuDFColumn> + 'a {
        deferred(move |ctx| sort::stable_sorted_order_on_context(ctx, self, sort_orders))
    }
}

impl Clone for CuDFTableView {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            inner: self.inner.clone(),
            stream_readiness: self.stream_readiness.clone(),
        }
    }
}

impl CuDFStreamReady for CuDFTableView {
    fn wait_ready_on_stream(&self, stream: &ffi::CudaStreamView) -> Result<(), CuDFError> {
        self.stream_readiness.wait_on_stream(stream)
    }
}
