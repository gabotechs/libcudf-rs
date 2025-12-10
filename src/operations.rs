use crate::{CuDFColumnView, CuDFError, CuDFRef, CuDFTable, CuDFTableView};
use libcudf_sys::ffi;
use std::sync::Arc;

/// Gather rows from a table based on a gather map
///
/// Reorders the rows of a table according to the indices specified in the gather map.
/// This is useful for applying a sort order or selecting specific rows.
///
/// # Arguments
///
/// * `table` - The table view to gather rows from
/// * `gather_map` - A column of indices indicating which rows to gather
///
/// # Errors
///
/// Returns an error if:
/// - The gather map contains out-of-bounds indices
/// - There is insufficient GPU memory
///
/// # Examples
///
/// ```no_run
/// use libcudf_rs::{CuDFTable, SortOrder, stable_sorted_order, gather};
///
/// let table = CuDFTable::from_parquet("data.parquet")?;
/// let view = table.into_view();
///
/// // Get sorted indices
/// let sort_orders = vec![SortOrder::AscendingNullsLast];
/// let indices = stable_sorted_order(&view, &sort_orders)?;
/// let indices_view = std::sync::Arc::new(indices).view();
///
/// // Apply the sort order
/// let sorted = gather(&view, &indices_view)?;
/// # Ok::<(), libcudf_rs::CuDFError>(())
/// ```
pub fn gather(
    table: &CuDFTableView,
    gather_map: &CuDFColumnView,
) -> Result<CuDFTable, CuDFError> {
    let inner = ffi::gather(table.inner(), gather_map.inner())?;
    Ok(CuDFTable::from_inner(inner))
}

/// Filter a table using a boolean mask
///
/// Returns a new table containing only the rows where the corresponding
/// element in the boolean mask is `true`. This operation is stable: the
/// input order is preserved in the output.
///
/// # Arguments
///
/// * `table` - The table view to filter
/// * `boolean_mask` - A boolean column where `true` indicates rows to keep
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
/// use libcudf_rs::{apply_boolean_mask, CuDFColumn, CuDFTable};
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
/// let mask_column = CuDFColumn::from_arrow_host(&mask)?;
/// let mask_view = Arc::new(mask_column).view();
///
/// // Filter the table
/// let table_view = table.into_view();
/// let filtered = apply_boolean_mask(&table_view, &mask_view)?;
/// assert_eq!(filtered.num_rows(), 3);
/// # Ok::<(), libcudf_rs::CuDFError>(())
/// ```
pub fn apply_boolean_mask(
    table: &CuDFTableView,
    boolean_mask: &CuDFColumnView,
) -> Result<CuDFTable, CuDFError> {
    let inner = ffi::apply_boolean_mask(table.inner(), boolean_mask.inner())?;
    Ok(CuDFTable::from_inner(inner))
}

/// Create a sliced view of a column
///
/// Returns a new column view that is a slice of the input column from `offset` to `offset + length`.
/// The returned view keeps the source column alive through reference counting.
///
/// # Arguments
///
/// * `column` - The column view to slice
/// * `offset` - The starting index of the slice
/// * `length` - The number of elements in the slice
///
/// # Errors
///
/// Returns an error if:
/// - `offset + length` exceeds the column size
/// - There is insufficient GPU memory
///
/// # Examples
///
/// ```no_run
/// use libcudf_rs::{CuDFColumn, slice_column};
/// use arrow::array::Int32Array;
/// use std::sync::Arc;
///
/// let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
/// let column = CuDFColumn::from_arrow_host(&array)?;
/// let column_view = Arc::new(column).view();
///
/// // Get elements 1, 2, 3 (indices 1-3)
/// let sliced = slice_column(&column_view, 1, 3)?;
/// # Ok::<(), libcudf_rs::CuDFError>(())
/// ```
pub fn slice_column(
    column: &CuDFColumnView,
    offset: usize,
    length: usize,
) -> Result<CuDFColumnView, CuDFError> {
    let inner = ffi::slice_column(column.inner(), offset, length)?;
    Ok(CuDFColumnView::new_with_ref(
        inner,
        Some(Arc::new(column.clone()) as Arc<dyn CuDFRef>),
    ))
}
