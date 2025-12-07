mod pretty;

#[cfg(test)]
mod tests {
    use crate::pretty::pretty_column;
    use arrow_schema::DataType;
    use insta::assert_snapshot;
    use libcudf_sys::ffi;

    #[test]
    fn test_slice_column_basic() -> Result<(), Box<dyn std::error::Error>> {
        // Read a parquet file to get test data
        let table = ffi::read_parquet("../testdata/weather/result-000000.parquet")?;
        let table_view = table.view();
        let original_col = table_view.column(1); // MinTemp column

        let original_size = original_col.size();
        assert!(original_size > 10, "Need at least 10 rows for testing");

        // Slice from offset 5, length 5
        let sliced_col = original_col.slice(5, 5);

        // Verify size
        assert_eq!(sliced_col.size(), 5);

        // Verify data type is preserved
        let original_dtype = original_col.data_type();
        let sliced_dtype = sliced_col.data_type();
        assert_eq!(original_dtype.id(), sliced_dtype.id());

        // Snapshot the sliced column
        assert_snapshot!(pretty_column(&sliced_col, DataType::Float64)?, @r"
        +------+
        | test |
        +------+
        | 16.9 |
        | 18.2 |
        | 17.0 |
        | 19.5 |
        | 22.8 |
        +------+
        ");

        Ok(())
    }

    #[test]
    fn test_slice_column_from_start() -> Result<(), Box<dyn std::error::Error>> {
        let table = ffi::read_parquet("../testdata/weather/result-000000.parquet")?;
        let table_view = table.view();
        let original_col = table_view.column(2); // MaxTemp column

        // Slice from the beginning
        let sliced_col = original_col.slice(0, 10);

        assert_eq!(sliced_col.size(), 10);
        assert_snapshot!(pretty_column(&sliced_col, DataType::Float64)?, @r"
        +------+
        | test |
        +------+
        | 0.0  |
        | 3.6  |
        | 3.6  |
        | 39.8 |
        | 2.8  |
        | 0.0  |
        | 0.2  |
        | 0.0  |
        | 0.0  |
        | 16.2 |
        +------+
        ");

        Ok(())
    }

    #[test]
    fn test_slice_column_to_end() -> Result<(), Box<dyn std::error::Error>> {
        let table = ffi::read_parquet("../testdata/weather/result-000000.parquet")?;
        let table_view = table.view();
        let original_col = table_view.column(1);

        let total_size = original_col.size();

        // Slice the last 10 rows
        let sliced_col = original_col.slice(total_size - 10, 10);

        assert_eq!(sliced_col.size(), 10);
        assert_snapshot!(pretty_column(&sliced_col, DataType::Float64)?, @r"
        +------+
        | test |
        +------+
        | 24.8 |
        | 28.6 |
        | 25.1 |
        | 23.8 |
        | 25.9 |
        | 28.2 |
        | 27.6 |
        | 17.3 |
        | 18.4 |
        | 21.8 |
        +------+
        ");

        Ok(())
    }

    #[test]
    fn test_slice_column_single_element() -> Result<(), Box<dyn std::error::Error>> {
        let table = ffi::read_parquet("../testdata/weather/result-000000.parquet")?;
        let table_view = table.view();
        let original_col = table_view.column(1);

        // Slice just one element
        let sliced_col = original_col.slice(5, 1);

        assert_eq!(sliced_col.size(), 1);
        assert_snapshot!(pretty_column(&sliced_col, DataType::Float64)?, @r"
        +------+
        | test |
        +------+
        | 16.9 |
        +------+
        ");

        Ok(())
    }

    #[test]
    fn test_slice_column_full_length() -> Result<(), Box<dyn std::error::Error>> {
        let table = ffi::read_parquet("../testdata/weather/result-000000.parquet")?;
        let table_view = table.view();
        let original_col = table_view.column(1);

        let total_size = original_col.size();

        // Slice the entire column
        let sliced_col = original_col.slice(0, total_size);

        assert_eq!(sliced_col.size(), total_size);

        Ok(())
    }

    #[test]
    fn test_slice_multiple_columns() -> Result<(), Box<dyn std::error::Error>> {
        let table = ffi::read_parquet("../testdata/weather/result-000000.parquet")?;
        let table_view = table.view();

        // Slice multiple columns with the same range
        let min_temp = table_view.column(1);
        let max_temp = table_view.column(2);

        let sliced_min = min_temp.slice(10, 5);
        let sliced_max = max_temp.slice(10, 5);

        assert_eq!(sliced_min.size(), 5);
        assert_eq!(sliced_max.size(), 5);

        assert_snapshot!(pretty_column(&sliced_min, DataType::Float64)?, @r"
        +------+
        | test |
        +------+
        | 25.2 |
        | 27.3 |
        | 27.9 |
        | 30.9 |
        | 31.2 |
        +------+
        ");
        assert_snapshot!(pretty_column(&sliced_max, DataType::Float64)?, @r"
        +------+
        | test |
        +------+
        | 0.0  |
        | 0.2  |
        | 0.0  |
        | 0.0  |
        | 0.0  |
        +------+
        ");

        Ok(())
    }

    #[test]
    fn test_slice_chained() -> Result<(), Box<dyn std::error::Error>> {
        // Test that we can slice a slice
        let table = ffi::read_parquet("../testdata/weather/result-000000.parquet")?;
        let table_view = table.view();
        let original_col = table_view.column(1);

        // First slice: rows 10-30 (20 rows)
        let first_slice = original_col.slice(10, 20);
        assert_eq!(first_slice.size(), 20);

        // Second slice: rows 5-15 of the first slice (10 rows)
        // This corresponds to rows 15-25 of the original
        let second_slice = first_slice.slice(5, 10);
        assert_eq!(second_slice.size(), 10);

        assert_snapshot!(pretty_column(&second_slice, DataType::Float64)?, @r"
        +------+
        | test |
        +------+
        | 32.1 |
        | 31.2 |
        | 30.0 |
        | 32.3 |
        | 33.4 |
        | 33.4 |
        | 19.4 |
        | 18.5 |
        | 24.3 |
        | 28.4 |
        +------+
        ");

        Ok(())
    }
}
