mod common;

#[cfg(test)]
mod tests {
    use libcudf_sys::{ffi, NullOrder, Order};

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
    fn test_multi_column_sort() -> Result<(), Box<dyn std::error::Error>> {
        // Read a parquet file
        let table = ffi::read_parquet("../testdata/weather/result-000000.parquet")?;
        let table_view = table.view();

        // Sort by all columns with different orders
        let num_cols = table.num_columns();
        let mut column_order = Vec::new();
        let mut null_precedence = Vec::new();

        for i in 0..num_cols {
            // Alternate between ascending and descending
            if i % 2 == 0 {
                column_order.push(Order::Ascending as i32);
                null_precedence.push(NullOrder::Before as i32);
            } else {
                column_order.push(Order::Descending as i32);
                null_precedence.push(NullOrder::After as i32);
            }
        }

        let sorted_table = ffi::sort_table(&table_view, &column_order, &null_precedence)?;

        assert_eq!(sorted_table.num_rows(), table.num_rows());
        assert_eq!(sorted_table.num_columns(), table.num_columns());

        Ok(())
    }

    #[test]
    fn test_order_enum() {
        assert_eq!(Order::Ascending as i32, 0);
        assert_eq!(Order::Descending as i32, 1);
    }

    #[test]
    fn test_null_order_enum() {
        assert_eq!(NullOrder::After as i32, 0);
        assert_eq!(NullOrder::Before as i32, 1);
    }
}
