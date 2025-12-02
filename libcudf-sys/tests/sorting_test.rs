mod pretty;

use arrow_schema::DataType;
use insta::assert_snapshot;
use libcudf_sys::{ffi, NullOrder, Order};
use pretty::{pretty_column, pretty_table};

#[test]
fn test_sort_table_ascending() -> Result<(), Box<dyn std::error::Error>> {
    // Read a parquet file
    let table = ffi::read_parquet("../testdata/weather/result-000000.parquet")?;
    let table_view = table.view();

    // Sort by all columns in ascending order
    let num_cols = table.num_columns();
    let column_order: Vec<i32> = vec![Order::Ascending as i32; num_cols];
    let null_precedence: Vec<i32> = vec![NullOrder::Before as i32; num_cols];

    let sorted_table = ffi::sort_table(&table_view, &column_order, &null_precedence)?;

    // Verify the sorted table has the same dimensions
    assert_eq!(sorted_table.num_rows(), table.num_rows());
    assert_eq!(sorted_table.num_columns(), table.num_columns());
    assert_snapshot!(pretty_table(&sorted_table.view())?);

    Ok(())
}

#[test]
fn test_sort_table_descending() -> Result<(), Box<dyn std::error::Error>> {
    // Read a parquet file
    let table = ffi::read_parquet("../testdata/weather/result-000000.parquet")?;
    let table_view = table.view();

    // Sort by all columns in descending order
    let num_cols = table.num_columns();
    let column_order: Vec<i32> = vec![Order::Descending as i32; num_cols];
    let null_precedence: Vec<i32> = vec![NullOrder::After as i32; num_cols];

    let sorted_table = ffi::sort_table(&table_view, &column_order, &null_precedence)?;

    // Verify the sorted table has the same dimensions
    assert_eq!(sorted_table.num_rows(), table.num_rows());
    assert_eq!(sorted_table.num_columns(), table.num_columns());
    assert_snapshot!(pretty_table(&sorted_table.view())?);

    Ok(())
}

#[test]
fn test_stable_sort_table() -> Result<(), Box<dyn std::error::Error>> {
    // Read a parquet file
    let table = ffi::read_parquet("../testdata/weather/result-000000.parquet")?;
    let table_view = table.view();

    // Stable sort by all columns
    let num_cols = table.num_columns();
    let column_order: Vec<i32> = vec![Order::Ascending as i32; num_cols];
    let null_precedence: Vec<i32> = vec![NullOrder::Before as i32; num_cols];

    let sorted_table = ffi::stable_sort_table(&table_view, &column_order, &null_precedence)?;

    // Verify the sorted table has the same dimensions
    assert_eq!(sorted_table.num_rows(), table.num_rows());
    assert_eq!(sorted_table.num_columns(), table.num_columns());
    assert_snapshot!(pretty_table(&sorted_table.view())?);

    Ok(())
}

#[test]
fn test_sorted_order() -> Result<(), Box<dyn std::error::Error>> {
    // Read a parquet file
    let table = ffi::read_parquet("../testdata/weather/result-000000.parquet")?;
    let table_view = table.view();

    // Get sort indices
    let num_cols = table.num_columns();
    let column_order: Vec<i32> = vec![Order::Ascending as i32; num_cols];
    let null_precedence: Vec<i32> = vec![NullOrder::Before as i32; num_cols];

    let indices = ffi::sorted_order(&table_view, &column_order, &null_precedence)?;

    // Verify the indices column has the right number of rows
    assert_eq!(indices.size(), table.num_rows());
    assert_snapshot!(pretty_column(&indices.view(), DataType::Int32)?);

    Ok(())
}

#[test]
fn test_is_sorted() -> Result<(), Box<dyn std::error::Error>> {
    // Read a parquet file
    let table = ffi::read_parquet("../testdata/weather/result-000000.parquet")?;
    let table_view = table.view();

    // Sort the table first
    let num_cols = table.num_columns();
    let column_order: Vec<i32> = vec![Order::Ascending as i32; num_cols];
    let null_precedence: Vec<i32> = vec![NullOrder::Before as i32; num_cols];

    let sorted_table = ffi::sort_table(&table_view, &column_order, &null_precedence)?;
    let sorted_view = sorted_table.view();

    // Check if it's sorted (it should be)
    let is_sorted = ffi::is_sorted(&sorted_view, &column_order, &null_precedence)?;
    assert!(is_sorted, "Table should be sorted after calling sort_table");

    Ok(())
}

#[test]
fn test_sort_by_key() -> Result<(), Box<dyn std::error::Error>> {
    // Read a parquet file
    let table = ffi::read_parquet("../testdata/weather/result-000000.parquet")?;
    let table_view = table.view();

    // Sort by only the first column - use select to create a view with just that column
    let keys_view = table_view.select(&[0]);

    // Provide ordering for just the one key column
    let column_order = vec![Order::Ascending as i32];
    let null_precedence = vec![NullOrder::Before as i32];

    let sorted_table = ffi::sort_by_key(&table_view, &keys_view, &column_order, &null_precedence)?;

    // Verify dimensions
    assert_eq!(sorted_table.num_rows(), table.num_rows());
    assert_eq!(sorted_table.num_columns(), table.num_columns());
    assert_snapshot!(pretty_table(&sorted_table.view())?);

    Ok(())
}

#[test]
fn test_stable_sort_by_key() -> Result<(), Box<dyn std::error::Error>> {
    // Read a parquet file
    let table = ffi::read_parquet("../testdata/weather/result-000000.parquet")?;
    let table_view = table.view();

    // Sort by the first two columns - use select to create a view with just those columns
    let keys_view = table_view.select(&[0, 1]);

    // Provide ordering for the two key columns
    let column_order = vec![Order::Ascending as i32, Order::Descending as i32];
    let null_precedence = vec![NullOrder::Before as i32, NullOrder::After as i32];

    let sorted_table =
        ffi::stable_sort_by_key(&table_view, &keys_view, &column_order, &null_precedence)?;

    // Verify dimensions
    assert_eq!(sorted_table.num_rows(), table.num_rows());
    assert_eq!(sorted_table.num_columns(), table.num_columns());
    assert_snapshot!(pretty_table(&sorted_table.view())?);

    Ok(())
}
