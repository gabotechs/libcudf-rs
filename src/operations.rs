use crate::data_type::arrow_type_to_cudf_data_type;
use crate::{CuDFColumn, CuDFColumnView, CuDFError, CuDFRef, CuDFTable, CuDFTableView};
use arrow_schema::{ArrowError, DataType};
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
pub fn gather(table: &CuDFTableView, gather_map: &CuDFColumnView) -> Result<CuDFTable, CuDFError> {
    let stream = ffi::get_default_stream();
    let mr = ffi::get_current_device_resource_ref();
    let inner = ffi::gather(
        table.inner(),
        gather_map.inner(),
        crate::stream::stream_ref(&stream)?,
        crate::stream::resource_ref(&mr)?,
    )?;
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
    let stream = ffi::get_default_stream();
    let mr = ffi::get_current_device_resource_ref();
    let inner = ffi::apply_boolean_mask(
        table.inner(),
        boolean_mask.inner(),
        crate::stream::stream_ref(&stream)?,
        crate::stream::resource_ref(&mr)?,
    )?;
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
    let stream = ffi::get_default_stream();
    let inner = ffi::slice_column(
        column.inner(),
        offset,
        length,
        crate::stream::stream_ref(&stream)?,
    )?;
    Ok(CuDFColumnView::new_with_ref(
        inner,
        Some(Arc::new(column.clone()) as Arc<dyn CuDFRef>),
    ))
}

/// Cast a column to a different data type on the GPU
///
/// Uses cuDF's native `cudf::cast()` to perform type conversion entirely on the GPU,
/// avoiding any GPU->CPU->GPU round-trips.
///
/// # Arguments
///
/// * `column` - The column view to cast
/// * `target_type` - The Arrow data type to cast to
///
/// # Errors
///
/// Returns an error if:
/// - The target type is not supported by cuDF
/// - The cast is not supported (e.g., string to numeric)
/// - There is insufficient GPU memory
pub fn cast(column: &CuDFColumnView, target_type: &DataType) -> Result<CuDFColumn, CuDFError> {
    let cudf_dt = arrow_type_to_cudf_data_type(target_type).ok_or_else(|| {
        CuDFError::ArrowError(ArrowError::NotYetImplemented(format!(
            "Arrow type {} not supported in cuDF cast",
            target_type
        )))
    })?;
    let stream = ffi::get_default_stream();
    let mr = ffi::get_current_device_resource_ref();
    let result = ffi::cast_column(
        column.inner(),
        &cudf_dt,
        crate::stream::stream_ref(&stream)?,
        crate::stream::resource_ref(&mr)?,
    )?;
    Ok(CuDFColumn::new(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;

    #[test]
    fn test_cast_int32_to_int64() -> Result<(), Box<dyn std::error::Error>> {
        let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let column = CuDFColumn::from_arrow_host(&array)?.into_view();

        let casted = cast(&column, &DataType::Int64)?;
        let result = casted.into_view().to_arrow_host()?;

        let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(result.values(), &[1i64, 2, 3, 4, 5]);
        Ok(())
    }

    #[test]
    fn test_cast_int32_to_float64() -> Result<(), Box<dyn std::error::Error>> {
        let array = Int32Array::from(vec![1, 2, 3]);
        let column = CuDFColumn::from_arrow_host(&array)?.into_view();

        let casted = cast(&column, &DataType::Float64)?;
        let result = casted.into_view().to_arrow_host()?;

        let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(result.values(), &[1.0, 2.0, 3.0]);
        Ok(())
    }

    #[test]
    fn test_cast_float64_to_int64() -> Result<(), Box<dyn std::error::Error>> {
        let array = Float64Array::from(vec![1.9, 2.1, 3.5]);
        let column = CuDFColumn::from_arrow_host(&array)?.into_view();

        let casted = cast(&column, &DataType::Int64)?;
        let result = casted.into_view().to_arrow_host()?;

        let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
        // cuDF truncates toward zero
        assert_eq!(result.values(), &[1i64, 2, 3]);
        Ok(())
    }

    #[test]
    fn test_cast_with_nulls() -> Result<(), Box<dyn std::error::Error>> {
        let array = Int32Array::from(vec![Some(1), None, Some(3), None, Some(5)]);
        let column = CuDFColumn::from_arrow_host(&array)?.into_view();

        let casted = cast(&column, &DataType::Int64)?;
        let view = casted.into_view();
        let result = view.to_arrow_host()?;

        let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(result.len(), 5);
        assert!(result.is_valid(0));
        assert!(result.is_null(1));
        assert!(result.is_valid(2));
        assert!(result.is_null(3));
        assert!(result.is_valid(4));
        assert_eq!(result.value(0), 1);
        assert_eq!(result.value(2), 3);
        assert_eq!(result.value(4), 5);
        Ok(())
    }

    #[test]
    fn test_cast_int64_to_uint64() -> Result<(), Box<dyn std::error::Error>> {
        let array = Int64Array::from(vec![1, 2, 3]);
        let column = CuDFColumn::from_arrow_host(&array)?.into_view();

        let casted = cast(&column, &DataType::UInt64)?;
        let result = casted.into_view().to_arrow_host()?;

        let result = result.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(result.values(), &[1u64, 2, 3]);
        Ok(())
    }

    #[test]
    fn test_cast_preserves_length() -> Result<(), Box<dyn std::error::Error>> {
        let array = Int32Array::from(vec![10, 20, 30, 40, 50]);
        let column = CuDFColumn::from_arrow_host(&array)?.into_view();

        let casted = cast(&column, &DataType::Float32)?;
        let view = casted.into_view();
        assert_eq!(view.len(), 5);
        Ok(())
    }

    #[test]
    fn test_cast_unsupported_type_returns_error() -> Result<(), Box<dyn std::error::Error>> {
        let array = Int32Array::from(vec![1, 2, 3]);
        let column = CuDFColumn::from_arrow_host(&array)?.into_view();

        // Interval types are not supported by cuDF
        let result = cast(&column, &DataType::Null);
        assert!(result.is_err());
        Ok(())
    }
}
