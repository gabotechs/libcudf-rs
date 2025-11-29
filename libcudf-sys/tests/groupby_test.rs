#[cfg(test)]
mod tests {
    use libcudf_sys::ffi;

    #[test]
    fn test_groupby_sum() -> Result<(), Box<dyn std::error::Error>> {
        let table = ffi::read_parquet("../testdata/weather/result-000000.parquet")?;
        let table_view = table.view();

        // Group by first column (index 0), aggregate second column (index 1)
        let groupby = ffi::groupby_create(&table_view.select(&[0]));

        let value_column = table_view.column(1);
        let mut request = ffi::aggregation_request_create(&value_column);
        request.pin_mut().add(ffi::make_sum_aggregation_groupby());

        // Execute groupby
        let requests = &[&*request as *const ffi::AggregationRequest];
        let mut result = groupby.aggregate(requests)?;

        // Verify results
        assert_eq!(result.results_size(), 1);
        let agg_result = result.pin_mut().get_result(0);
        assert_eq!(agg_result.results_size(), 1);

        let keys = result.get_keys();
        assert!(keys.num_rows() > 0);

        Ok(())
    }

    #[test]
    fn test_groupby_multiple_aggregations() -> Result<(), Box<dyn std::error::Error>> {
        let table = ffi::read_parquet("../testdata/weather/result-000001.parquet")?;
        let table_view = table.view();

        let keys_view = table_view.select(&[0]);
        let groupby = ffi::groupby_create(&keys_view);

        let value_column = table_view.column(1);
        let mut request = ffi::aggregation_request_create(&value_column);
        request.pin_mut().add(ffi::make_sum_aggregation_groupby());
        request.pin_mut().add(ffi::make_min_aggregation_groupby());
        request.pin_mut().add(ffi::make_max_aggregation_groupby());

        let requests = &[&*request as *const ffi::AggregationRequest];
        let mut result = groupby.aggregate(requests)?;

        assert_eq!(result.results_size(), 1);
        let agg_result = result.pin_mut().get_result(0);
        assert_eq!(agg_result.results_size(), 3);

        let keys = result.get_keys();
        assert!(keys.num_rows() > 0);

        Ok(())
    }

    #[test]
    fn test_groupby_count() -> Result<(), Box<dyn std::error::Error>> {
        let table = ffi::read_parquet("../testdata/weather/result-000002.parquet")?;
        let table_view = table.view();

        let keys_view = table_view.select(&[0]);
        let groupby = ffi::groupby_create(&keys_view);

        let value_column = table_view.column(1);
        let mut request = ffi::aggregation_request_create(&value_column);
        request.pin_mut().add(ffi::make_count_aggregation_groupby());

        let requests = &[&*request as *const ffi::AggregationRequest];
        let result = groupby.aggregate(requests)?;

        let keys = result.get_keys();
        assert!(keys.num_rows() > 0);

        Ok(())
    }

    #[test]
    fn test_read_parquet_basic() -> Result<(), Box<dyn std::error::Error>> {
        let table = ffi::read_parquet("../testdata/weather/result-000000.parquet")?;

        assert!(table.num_rows() > 0);
        assert!(table.num_columns() > 0);

        Ok(())
    }
}
