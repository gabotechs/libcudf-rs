use crate::data_type::arrow_type_to_cudf_data_type;
use crate::device_resource::resource_ref;
use crate::stream::stream_ref;
use crate::{CuDFColumn, CuDFColumnView, CuDFError, CuDFRef, CuDFTable, CuDFTableView};
use arrow::array::Array;
use arrow_schema::{ArrowError, DataType};
use libcudf_sys::{ffi, OutOfBoundsPolicy};
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
/// Out-of-bounds indices produce null rows.
///
/// # Errors
///
/// Returns an error if the gather map is invalid or GPU execution fails.
///
/// # Examples
///
/// ```no_run
/// use libcudf_rs::{CuDFTable, SortOrder, stable_sorted_order, gather};
///
/// let table = CuDFTable::read_parquet("data.parquet")?;
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
    gather_with_policy(table, gather_map, OutOfBoundsPolicy::Nullify)
}

/// Gather rows without checking whether indices are in bounds.
///
/// This avoids the bounds check performed by [`gather`] when the producer of
/// `gather_map` already guarantees valid indices.
///
/// # Safety
///
/// Every index in `gather_map` must be in `[-table.num_rows(), table.num_rows())`.
/// Violating this precondition causes undefined behavior in cuDF.
///
/// # Errors
///
/// Returns an error if the gather map is otherwise invalid or GPU execution fails.
pub unsafe fn gather_unchecked(
    table: &CuDFTableView,
    gather_map: &CuDFColumnView,
) -> Result<CuDFTable, CuDFError> {
    gather_with_policy(table, gather_map, OutOfBoundsPolicy::DontCheck)
}

fn gather_with_policy(
    table: &CuDFTableView,
    gather_map: &CuDFColumnView,
    policy: OutOfBoundsPolicy,
) -> Result<CuDFTable, CuDFError> {
    let stream = ffi::get_default_stream();
    let mr = ffi::get_current_device_resource_ref();
    let inner = ffi::gather(
        table.inner(),
        gather_map.inner(),
        policy as i32,
        stream_ref(&stream)?,
        resource_ref(&mr)?,
    )?;
    CuDFTable::try_from_inner(inner)
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
/// let table = CuDFTable::try_from_arrow_host(batch)?;
///
/// // Create a boolean mask
/// let mask = BooleanArray::from(vec![true, false, true, false, true]);
/// let mask_column = CuDFColumn::try_from_arrow_host(&mask)?;
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
        stream_ref(&stream)?,
        resource_ref(&mr)?,
    )?;
    CuDFTable::try_from_inner(inner)
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
/// let column = CuDFColumn::try_from_arrow_host(&array)?;
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
    let end = offset.checked_add(length).ok_or_else(|| {
        ArrowError::InvalidArgumentError("column slice end overflowed usize".to_string())
    })?;
    if end > column.len() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "column slice [{offset}, {end}) exceeds length {}",
            column.len()
        ))
        .into());
    }
    let indices = [
        crate::errors::usize_to_cudf_size(offset, "column slice offset")?,
        crate::errors::usize_to_cudf_size(end, "column slice end")?,
    ];
    let stream = ffi::get_default_stream();
    // SAFETY: the returned view is attached to a clone of `column` below.
    let views = unsafe { ffi::slice_column(column.inner(), &indices, stream_ref(&stream)?) }?;
    let views = views
        .as_ref()
        .ok_or(CuDFError::NullHandle("column view vector"))?;
    if views.len() != 1 {
        return Err(ArrowError::ComputeError(format!(
            "cuDF returned {} views for one slice interval",
            views.len()
        ))
        .into());
    }
    // SAFETY: the result keeps `column` alive for the lifetime of the view.
    let inner = unsafe { views.get(0) }?;
    CuDFColumnView::try_from_inner(inner, Some(Arc::new(column.clone()) as Arc<dyn CuDFRef>))
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
    let result = ffi::cast(
        column.inner(),
        &cudf_dt,
        stream_ref(&stream)?,
        resource_ref(&mr)?,
    )?;
    CuDFColumn::try_from_inner(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use arrow_schema::{Field, Schema};

    #[test]
    fn gather_nullifies_out_of_bounds_indices() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Schema::new(vec![Field::new("value", DataType::Int32, false)]);
        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int32Array::from(vec![10, 20, 30]))],
        )?;
        let table = CuDFTable::try_from_arrow_host(batch)?.into_view();
        let gather_map = Arc::new(CuDFColumn::try_from_arrow_host(&Int32Array::from(vec![
            2, 5,
        ]))?)
        .view();

        let result = gather(&table, &gather_map)?.into_view().to_arrow_host()?;
        let values = result
            .column(0)
            .as_primitive::<arrow::datatypes::Int32Type>();
        assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some(30), None]);
        Ok(())
    }

    #[test]
    fn gather_unchecked_accepts_known_valid_indices() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Schema::new(vec![Field::new("value", DataType::Int32, false)]);
        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int32Array::from(vec![10, 20, 30]))],
        )?;
        let table = CuDFTable::try_from_arrow_host(batch)?.into_view();
        let gather_map = Arc::new(CuDFColumn::try_from_arrow_host(&Int32Array::from(vec![
            2, 0,
        ]))?)
        .view();

        // SAFETY: both gather indices are within the three-row input table.
        let result = unsafe { gather_unchecked(&table, &gather_map) }?
            .into_view()
            .to_arrow_host()?;
        let values = result
            .column(0)
            .as_primitive::<arrow::datatypes::Int32Type>();
        assert_eq!(values.values(), &[30, 10]);
        Ok(())
    }

    #[test]
    fn test_cast_int32_to_int64() -> Result<(), Box<dyn std::error::Error>> {
        let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let column = CuDFColumn::try_from_arrow_host(&array)?.into_view();

        let casted = cast(&column, &DataType::Int64)?;
        let result = casted.into_view().to_arrow_host()?;

        let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(result.values(), &[1i64, 2, 3, 4, 5]);
        Ok(())
    }

    #[test]
    fn test_cast_int32_to_float64() -> Result<(), Box<dyn std::error::Error>> {
        let array = Int32Array::from(vec![1, 2, 3]);
        let column = CuDFColumn::try_from_arrow_host(&array)?.into_view();

        let casted = cast(&column, &DataType::Float64)?;
        let result = casted.into_view().to_arrow_host()?;

        let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(result.values(), &[1.0, 2.0, 3.0]);
        Ok(())
    }

    #[test]
    fn test_cast_float64_to_int64() -> Result<(), Box<dyn std::error::Error>> {
        let array = Float64Array::from(vec![1.9, 2.1, 3.5]);
        let column = CuDFColumn::try_from_arrow_host(&array)?.into_view();

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
        let column = CuDFColumn::try_from_arrow_host(&array)?.into_view();

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
        let column = CuDFColumn::try_from_arrow_host(&array)?.into_view();

        let casted = cast(&column, &DataType::UInt64)?;
        let result = casted.into_view().to_arrow_host()?;

        let result = result.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(result.values(), &[1u64, 2, 3]);
        Ok(())
    }

    #[test]
    fn test_cast_preserves_length() -> Result<(), Box<dyn std::error::Error>> {
        let array = Int32Array::from(vec![10, 20, 30, 40, 50]);
        let column = CuDFColumn::try_from_arrow_host(&array)?.into_view();

        let casted = cast(&column, &DataType::Float32)?;
        let view = casted.into_view();
        assert_eq!(view.len(), 5);
        Ok(())
    }

    #[test]
    fn test_cast_unsupported_type_returns_error() -> Result<(), Box<dyn std::error::Error>> {
        let array = Int32Array::from(vec![1, 2, 3]);
        let column = CuDFColumn::try_from_arrow_host(&array)?.into_view();

        // Interval types are not supported by cuDF
        let result = cast(&column, &DataType::Null);
        assert!(result.is_err());
        Ok(())
    }
}
