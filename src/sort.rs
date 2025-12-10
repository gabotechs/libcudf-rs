use crate::{CuDFColumn, CuDFError, CuDFTable, CuDFTableView};
use libcudf_sys::{ffi, NullOrder, Order};

/// Sort options combining sort order and null precedence
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SortOrder {
    /// Sort ascending with nulls first
    AscendingNullsFirst,
    /// Sort ascending with nulls last
    AscendingNullsLast,
    /// Sort descending with nulls first
    DescendingNullsFirst,
    /// Sort descending with nulls last
    DescendingNullsLast,
}

impl SortOrder {
    pub fn order(self) -> Order {
        match self {
            SortOrder::AscendingNullsFirst | SortOrder::AscendingNullsLast => Order::Ascending,
            SortOrder::DescendingNullsFirst | SortOrder::DescendingNullsLast => Order::Descending,
        }
    }

    pub fn null_order(self) -> NullOrder {
        match self {
            SortOrder::AscendingNullsFirst | SortOrder::DescendingNullsFirst => NullOrder::Before,
            SortOrder::AscendingNullsLast | SortOrder::DescendingNullsLast => NullOrder::After,
        }
    }
}

/// Sort a table by specified columns
///
/// Sorts the entire table based on the specified key columns.
/// This operation uses a stable sort algorithm, preserving the relative order of equivalent rows.
///
/// # Arguments
///
/// * `table` - The table view to sort
/// * `key_columns` - Indices of columns to sort by (in order of precedence)
/// * `sort_orders` - Sort order for each key column, specifying both direction and null handling
///
/// # Errors
///
/// Returns an error if:
/// - The sort_orders length doesn't match key_columns length
/// - Any key column index is out of bounds
/// - There is insufficient GPU memory
///
/// # Examples
///
/// ```no_run
/// use libcudf_rs::{CuDFTable, SortOrder, sort};
///
/// let table = CuDFTable::from_parquet("data.parquet")?;
/// let view = table.into_view();
///
/// // Sort by column 0 ascending, then column 2 descending as tiebreaker
/// let sorted = sort(&view, &[0, 2], &[SortOrder::AscendingNullsLast, SortOrder::DescendingNullsFirst])?;
/// # Ok::<(), libcudf_rs::CuDFError>(())
/// ```
pub fn sort(
    table: &CuDFTableView,
    key_columns: &[usize],
    sort_orders: &[SortOrder],
) -> Result<CuDFTable, CuDFError> {
    if key_columns.is_empty() {
        return Err(CuDFError::ArrowError(
            arrow::error::ArrowError::InvalidArgumentError(
                "key_columns cannot be empty".to_string(),
            ),
        ));
    }

    if key_columns.len() != sort_orders.len() {
        return Err(CuDFError::ArrowError(
            arrow::error::ArrowError::InvalidArgumentError(format!(
                "key_columns length ({}) must match sort_orders length ({})",
                key_columns.len(),
                sort_orders.len()
            )),
        ));
    }

    // Build keys table view from specified columns
    let key_views: Result<Vec<_>, _> = key_columns
        .iter()
        .map(|&idx| {
            if idx >= table.num_columns() {
                Err(CuDFError::ArrowError(
                    arrow::error::ArrowError::InvalidArgumentError(format!(
                        "column index {idx} out of bounds (table has {} columns)",
                        table.num_columns()
                    )),
                ))
            } else {
                Ok(table.column(idx as i32))
            }
        })
        .collect();
    let key_views = key_views?;
    let keys_view = CuDFTableView::from_column_views(key_views)?;

    let column_order_i32: Vec<i32> = sort_orders.iter().map(|&o| o.order() as i32).collect();
    let null_precedence_i32: Vec<i32> =
        sort_orders.iter().map(|&o| o.null_order() as i32).collect();

    let inner = ffi::stable_sort_by_key(
        table.inner(),
        keys_view.inner(),
        &column_order_i32,
        &null_precedence_i32,
    )?;
    Ok(CuDFTable::from_inner(inner))
}

/// Sort a table by all columns in lexicographic order
///
/// Sorts the rows of the table according to the specified sort orders for ALL columns.
/// This operation uses a stable sort algorithm, preserving the relative order of equivalent rows.
///
/// # Arguments
///
/// * `table` - The table view to sort
/// * `sort_orders` - Sort order for each column, specifying both direction and null handling
///
/// # Errors
///
/// Returns an error if:
/// - The sort_orders length doesn't match the number of columns
/// - There is insufficient GPU memory
///
/// # Examples
///
/// ```no_run
/// use libcudf_rs::{CuDFTable, SortOrder, sort_by_all};
///
/// let table = CuDFTable::from_parquet("data.parquet")?;
/// let view = table.into_view();
///
/// // Sort by first column ascending (nulls last), second column descending (nulls first)
/// let sort_orders = vec![SortOrder::AscendingNullsLast, SortOrder::DescendingNullsFirst];
/// let sorted = sort_by_all(&view, &sort_orders)?;
/// # Ok::<(), libcudf_rs::CuDFError>(())
/// ```
pub fn sort_by_all(
    table: &CuDFTableView,
    sort_orders: &[SortOrder],
) -> Result<CuDFTable, CuDFError> {
    let column_order_i32: Vec<i32> = sort_orders.iter().map(|&o| o.order() as i32).collect();
    let null_precedence_i32: Vec<i32> =
        sort_orders.iter().map(|&o| o.null_order() as i32).collect();

    let inner = ffi::stable_sort_table(table.inner(), &column_order_i32, &null_precedence_i32)?;
    Ok(CuDFTable::from_inner(inner))
}

/// Get the sorted order (indices) of a table
///
/// Returns a column of indices that would stably sort the table according to the specified sort orders.
/// This is useful for implementing top-K or when you need the sort order without actually reordering the data.
///
/// # Arguments
///
/// * `table` - The table view to compute sort order for
/// * `sort_orders` - Sort order for each column, specifying both direction and null handling
///
/// # Errors
///
/// Returns an error if there is insufficient GPU memory
///
/// # Examples
///
/// ```no_run
/// use libcudf_rs::{CuDFTable, SortOrder, stable_sorted_order};
///
/// let table = CuDFTable::from_parquet("data.parquet")?;
/// let view = table.into_view();
///
/// let sort_orders = vec![SortOrder::AscendingNullsLast, SortOrder::DescendingNullsFirst];
/// let indices = stable_sorted_order(&view, &sort_orders)?;
/// # Ok::<(), libcudf_rs::CuDFError>(())
/// ```
pub fn stable_sorted_order(
    table: &CuDFTableView,
    sort_orders: &[SortOrder],
) -> Result<CuDFColumn, CuDFError> {
    let column_order_i32: Vec<i32> = sort_orders.iter().map(|&o| o.order() as i32).collect();
    let null_precedence_i32: Vec<i32> =
        sort_orders.iter().map(|&o| o.null_order() as i32).collect();

    let inner = ffi::stable_sorted_order(table.inner(), &column_order_i32, &null_precedence_i32)?;
    Ok(CuDFColumn::new(inner))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;

    #[test]
    fn test_sort_ascending() -> Result<(), Box<dyn std::error::Error>> {
        let batch = record_batch!(("a", Int32, [3, 1, 4, 1, 5, 9, 2, 6]))?;

        let table = CuDFTable::from_arrow_host(batch)?;
        let view = table.into_view();

        let sorted = sort(&view, &[0], &[SortOrder::AscendingNullsLast])?;
        let result = sorted.into_view().to_arrow_host()?;

        let col: &Int32Array = cast(result.column(0));

        assert_eq!(col.values(), &[1, 1, 2, 3, 4, 5, 6, 9]);
        Ok(())
    }

    #[test]
    fn test_sort_descending() -> Result<(), Box<dyn std::error::Error>> {
        let batch = record_batch!(("a", Int32, [3, 1, 4, 1, 5]))?;

        let table = CuDFTable::from_arrow_host(batch)?;
        let view = table.into_view();

        let sorted = sort(&view, &[0], &[SortOrder::DescendingNullsLast])?;
        let result = sorted.into_view().to_arrow_host()?;

        let col: &Int32Array = cast(result.column(0));

        assert_eq!(col.values(), &[5, 4, 3, 1, 1]);
        Ok(())
    }

    #[test]
    fn test_sort_multiple_columns() -> Result<(), Box<dyn std::error::Error>> {
        let batch = record_batch!(
            ("a", Int32, [3, 1, 3, 1, 2]),
            ("b", Int32, [10, 20, 30, 40, 50])
        )?;

        let table = CuDFTable::from_arrow_host(batch)?;
        let view = table.into_view();

        // Sort by column A ascending, then column B ascending
        let sorted = sort(
            &view,
            &[0, 1],
            &[SortOrder::AscendingNullsLast, SortOrder::AscendingNullsLast],
        )?;
        let result = sorted.into_view().to_arrow_host()?;

        let col_a: &Int32Array = cast(result.column(0));
        let col_b: &Int32Array = cast(result.column(1));

        // Expected: First sort by A, then by B within same A values
        // (1, 20), (1, 40), (2, 50), (3, 10), (3, 30)
        assert_eq!(col_a.values(), &[1, 1, 2, 3, 3]);
        assert_eq!(col_b.values(), &[20, 40, 50, 10, 30]);
        Ok(())
    }

    #[test]
    fn test_sort_first_column_only() -> Result<(), Box<dyn std::error::Error>> {
        let batch = record_batch!(
            ("a", Int32, [3, 1, 3, 1, 2]),
            ("b", Int32, [10, 40, 30, 20, 50])
        )?;

        let table = CuDFTable::from_arrow_host(batch)?;
        let view = table.into_view();

        // Sort by column A only - column B order should be preserved (stable sort)
        let sorted = sort(&view, &[0], &[SortOrder::AscendingNullsLast])?;
        let result = sorted.into_view().to_arrow_host()?;

        let col_a: &Int32Array = cast(result.column(0));
        let col_b: &Int32Array = cast(result.column(1));

        // Expected: Sort by A only, stable sort preserves original B order within same A
        // (1, 40), (1, 20), (2, 50), (3, 10), (3, 30)
        assert_eq!(col_a.values(), &[1, 1, 2, 3, 3]);
        assert_eq!(col_b.values(), &[40, 20, 50, 10, 30]);
        Ok(())
    }

    #[test]
    fn test_sort_by_all() -> Result<(), Box<dyn std::error::Error>> {
        let batch = record_batch!(
            ("a", Int32, [3, 1, 3, 1, 2]),
            ("b", Int32, [10, 40, 30, 20, 50])
        )?;

        let table = CuDFTable::from_arrow_host(batch)?;
        let view = table.into_view();

        // Sort by all columns
        let sorted = sort_by_all(
            &view,
            &[SortOrder::AscendingNullsLast, SortOrder::AscendingNullsLast],
        )?;
        let result = sorted.into_view().to_arrow_host()?;

        let col_a: &Int32Array = cast(result.column(0));
        let col_b: &Int32Array = cast(result.column(1));

        // Expected: Sort by A first, then by B as tiebreaker
        // (1, 20), (1, 40), (2, 50), (3, 10), (3, 30)
        assert_eq!(col_a.values(), &[1, 1, 2, 3, 3]);
        assert_eq!(col_b.values(), &[20, 40, 50, 10, 30]);
        Ok(())
    }

    fn cast<T: 'static + Array>(arr: &dyn Array) -> &T {
        arr.as_any().downcast_ref::<T>().unwrap()
    }
}
