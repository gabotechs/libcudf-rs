mod pretty;

#[cfg(test)]
mod tests {
    use crate::pretty::pretty_column;
    use arrow_schema::DataType;
    use insta::assert_snapshot;
    use libcudf_sys::{ffi, BinaryOperator, TypeId};

    #[test]
    fn test_apply_boolean_mask() -> Result<(), Box<dyn std::error::Error>> {
        // Read a parquet file to get some test data
        let table = ffi::read_parquet("../testdata/weather/result-000000.parquet")?;
        let table_view = table.view();

        // Get MinTemp and MaxTemp columns
        let min_temp = table_view.column(1);
        let max_temp = table_view.column(2);

        // Create a boolean mask: MinTemp < MaxTemp (should be true for most rows)
        let boolean_mask = ffi::binary_operation_col_col(
            &min_temp,
            &max_temp,
            BinaryOperator::Less as i32,
            TypeId::Bool8 as i32,
        )?;

        // Apply the boolean mask to filter the table
        let filtered_table = ffi::apply_boolean_mask(&table_view, &boolean_mask.view())?;

        // Verify the filtered table has fewer rows than the original
        assert!(filtered_table.num_rows() < table.num_rows());
        assert_eq!(filtered_table.num_columns(), table.num_columns());

        // Check the first column of the filtered result
        let filtered_view = filtered_table.view();
        let filtered_col = filtered_view.column(1);
        assert_snapshot!(pretty_column(&filtered_col, DataType::Float64)?);

        Ok(())
    }

    #[test]
    fn test_apply_boolean_mask_partial() -> Result<(), Box<dyn std::error::Error>> {
        // Read a parquet file
        let table = ffi::read_parquet("../testdata/weather/result-000000.parquet")?;
        let table_view = table.view();

        // Get MinTemp and MaxTemp columns
        let min_temp = table_view.column(1);
        let max_temp = table_view.column(2);

        // Create a boolean mask: MinTemp <= MaxTemp
        // (This will be true for some rows, false for others)
        let boolean_mask = ffi::binary_operation_col_col(
            &min_temp,
            &max_temp,
            BinaryOperator::LessEqual as i32,
            TypeId::Bool8 as i32,
        )?;

        // Apply the boolean mask
        let filtered_table = ffi::apply_boolean_mask(&table_view, &boolean_mask.view())?;

        // Result should have <= rows than original
        assert!(filtered_table.num_rows() <= table.num_rows());
        assert_eq!(filtered_table.num_columns(), table.num_columns());

        Ok(())
    }

    #[test]
    fn test_apply_boolean_mask_selective() -> Result<(), Box<dyn std::error::Error>> {
        // Read a parquet file
        let table = ffi::read_parquet("../testdata/weather/result-000000.parquet")?;
        let table_view = table.view();

        // Get MinTemp and MaxTemp columns
        let min_temp = table_view.column(1);
        let max_temp = table_view.column(2);

        // Create a boolean mask: MinTemp > MaxTemp (should be false for most/all rows)
        let boolean_mask = ffi::binary_operation_col_col(
            &min_temp,
            &max_temp,
            BinaryOperator::Greater as i32,
            TypeId::Bool8 as i32,
        )?;

        // Apply the boolean mask
        let filtered_table = ffi::apply_boolean_mask(&table_view, &boolean_mask.view())?;

        // MinTemp should never be greater than MaxTemp, so result should be empty or very small
        assert!(filtered_table.num_rows() < table.num_rows());
        assert_eq!(filtered_table.num_columns(), table.num_columns());

        Ok(())
    }
}
