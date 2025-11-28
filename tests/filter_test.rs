#[cfg(test)]
mod tests {
    use libcudf_rs::{Column, Table};

    #[test]
    fn test_filter_partial() {
        let table = Table::from_parquet("testdata/weather/result-000000.parquet")
            .expect("Failed to read parquet file");

        let original_rows = table.num_rows();
        let num_cols = table.num_columns();

        // Create a boolean mask: keep first 100 rows
        let mut mask_data = vec![false; original_rows];
        for i in 0..100.min(original_rows) {
            mask_data[i] = true;
        }

        let mask = Column::from_bools(&mask_data).expect("Failed to create mask");

        // Apply filter
        let filtered = table.filter(&mask).expect("Failed to filter table");

        assert_eq!(filtered.num_rows(), 100.min(original_rows));
        assert_eq!(filtered.num_columns(), num_cols);
    }

    #[test]
    fn test_filter_all_false() {
        let table = Table::from_parquet("testdata/weather/result-000000.parquet")
            .expect("Failed to read parquet file");

        let original_rows = table.num_rows();

        // Create a mask with all false values
        let mask_data = vec![false; original_rows];
        let mask = Column::from_bools(&mask_data).expect("Failed to create mask");

        // Apply filter - should result in empty table
        let filtered = table.filter(&mask).expect("Failed to filter table");

        assert_eq!(filtered.num_rows(), 0);
    }

    #[test]
    fn test_filter_all_true() {
        let table = Table::from_parquet("testdata/weather/result-000000.parquet")
            .expect("Failed to read parquet file");

        let original_rows = table.num_rows();

        // Create a mask with all true values
        let mask_data = vec![true; original_rows];
        let mask = Column::from_bools(&mask_data).expect("Failed to create mask");

        // Apply filter - should keep all rows
        let filtered = table.filter(&mask).expect("Failed to filter table");

        assert_eq!(filtered.num_rows(), original_rows);
    }

    #[test]
    fn test_filter_alternating() {
        let table = Table::from_parquet("testdata/weather/result-000000.parquet")
            .expect("Failed to read parquet file");

        let original_rows = table.num_rows();

        // Create alternating mask: true, false, true, false, ...
        let mask_data: Vec<bool> = (0..original_rows).map(|i| i % 2 == 0).collect();
        let mask = Column::from_bools(&mask_data).expect("Failed to create mask");

        // Apply filter
        let filtered = table.filter(&mask).expect("Failed to filter table");

        // Should keep roughly half the rows (rounded up)
        let expected_rows = (original_rows + 1) / 2;
        assert_eq!(filtered.num_rows(), expected_rows);
    }

    #[test]
    fn test_filter_preserves_columns() {
        let table = Table::from_parquet("testdata/weather/result-000000.parquet")
            .expect("Failed to read parquet file");

        let original_rows = table.num_rows();
        let original_cols = table.num_columns();

        // Keep only first row
        let mut mask_data = vec![false; original_rows];
        if original_rows > 0 {
            mask_data[0] = true;
        }

        let mask = Column::from_bools(&mask_data).expect("Failed to create mask");
        let filtered = table.filter(&mask).expect("Failed to filter table");

        // Should keep all columns
        assert_eq!(filtered.num_columns(), original_cols);
        if original_rows > 0 {
            assert_eq!(filtered.num_rows(), 1);
        }
    }

    #[test]
    fn test_create_boolean_column() {
        // Test creating boolean columns with various patterns
        let data = vec![true, false, true, true, false];
        let column = Column::from_bools(&data).expect("Failed to create column");
        assert_eq!(column.size(), 5);
    }

    #[test]
    fn test_create_empty_boolean_column() {
        // Test creating an empty boolean column
        let data: Vec<bool> = vec![];
        let column = Column::from_bools(&data).expect("Failed to create column");
        assert_eq!(column.size(), 0);
    }

    #[test]
    fn test_create_large_boolean_column() {
        // Test creating a large boolean column
        let data = vec![true; 10000];
        let column = Column::from_bools(&data).expect("Failed to create column");
        assert_eq!(column.size(), 10000);
    }
}
