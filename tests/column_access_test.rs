#[cfg(test)]
mod tests {
    use libcudf_rs::Table;

    #[test]
    fn test_select_columns() {
        let table = Table::from_parquet("testdata/weather/result-000000.parquet")
            .expect("Failed to read parquet file");

        let original_cols = table.num_columns();
        let original_rows = table.num_rows();

        // Select first 3 columns
        let selected = table.select(&[0, 1, 2]).expect("Failed to select columns");

        assert_eq!(selected.num_columns(), 3);
        assert_eq!(selected.num_rows(), original_rows);
        assert!(selected.num_columns() < original_cols);
    }

    #[test]
    fn test_select_single_column() {
        let table = Table::from_parquet("testdata/weather/result-000000.parquet")
            .expect("Failed to read parquet file");

        let original_rows = table.num_rows();

        // Select just one column
        let selected = table.select(&[0]).expect("Failed to select column");

        assert_eq!(selected.num_columns(), 1);
        assert_eq!(selected.num_rows(), original_rows);
    }

    #[test]
    fn test_select_out_of_bounds() {
        let table = Table::from_parquet("testdata/weather/result-000000.parquet")
            .expect("Failed to read parquet file");

        let num_cols = table.num_columns();

        // Try to select a column that doesn't exist
        let result = table.select(&[num_cols + 10]);
        assert!(result.is_err());
    }

    #[test]
    fn test_select_reorder_columns() {
        let table = Table::from_parquet("testdata/weather/result-000000.parquet")
            .expect("Failed to read parquet file");

        let original_cols = table.num_columns();

        if original_cols >= 3 {
            // Select columns in different order
            let selected = table.select(&[2, 0, 1]).expect("Failed to select columns");
            assert_eq!(selected.num_columns(), 3);
        }
    }

    #[test]
    fn test_get_column() {
        let table = Table::from_parquet("testdata/weather/result-000000.parquet")
            .expect("Failed to read parquet file");

        let original_rows = table.num_rows();

        // Get the first column
        let column = table.get_column(0).expect("Failed to get column");

        assert_eq!(column.size(), original_rows);
    }

    #[test]
    fn test_get_column_out_of_bounds() {
        let table = Table::from_parquet("testdata/weather/result-000000.parquet")
            .expect("Failed to read parquet file");

        let num_cols = table.num_columns();

        // Try to get a column that doesn't exist
        let result = table.get_column(num_cols + 10);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_all_columns() {
        let table = Table::from_parquet("testdata/weather/result-000000.parquet")
            .expect("Failed to read parquet file");

        let num_cols = table.num_columns();
        let num_rows = table.num_rows();

        // Get all columns
        for i in 0..num_cols {
            let column = table.get_column(i).expect("Failed to get column");
            assert_eq!(column.size(), num_rows);
        }
    }
}
