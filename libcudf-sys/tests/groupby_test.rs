mod pretty;

use libcudf_sys::ffi;

#[test]
fn test_groupby_sum() -> Result<(), Box<dyn std::error::Error>> {
    let table = ffi::read_parquet("../testdata/weather/result-000000.parquet")?;
    let table_view = table.view();

    // Group by first column (index 0), aggregate second column (index 1)
    let groupby = ffi::groupby_create(&table_view.select(&[21]));

    let value_column = table_view.column(1);
    let mut request = ffi::aggregation_request_create(&value_column);
    request.pin_mut().add(ffi::make_max_aggregation_groupby());

    // Execute groupby
    let agg_requests = &[&*request as *const ffi::AggregationRequest];
    let mut groupby_result = groupby.aggregate(agg_requests)?;

    // Test that we can access aggregation results
    let mut aggregation_result = groupby_result.pin_mut().release_result(0);
    let keys = groupby_result.pin_mut().release_keys();

    // Only one aggregation (max) was added, so we get one result column
    assert_eq!(aggregation_result.len(), 1);

    // Verify the column has the expected number of rows
    assert_eq!(
        aggregation_result.pin_mut().release(0).size(),
        keys.num_rows()
    );

    Ok(())
}

#[test]
fn test_groupby_multiple_aggregations() -> Result<(), Box<dyn std::error::Error>> {
    let table = ffi::read_parquet("../testdata/weather/result-000001.parquet")?;
    let table_view = table.view();

    let keys_view = table_view.select(&[0]);
    let groupby = ffi::groupby_create(&keys_view);

    let value_column = table_view.column(1);
    let mut agg_request = ffi::aggregation_request_create(&value_column);
    agg_request
        .pin_mut()
        .add(ffi::make_sum_aggregation_groupby());
    agg_request
        .pin_mut()
        .add(ffi::make_min_aggregation_groupby());
    agg_request
        .pin_mut()
        .add(ffi::make_max_aggregation_groupby());

    let requests = &[&*agg_request as *const ffi::AggregationRequest];
    let mut groupby_result = groupby.aggregate(requests)?;

    assert_eq!(groupby_result.len(), 1);
    let mut agg_result = groupby_result.pin_mut().release_result(0);
    assert_eq!(agg_result.len(), 3);

    // Access columns using the accessor methods
    let sum_column = agg_result.pin_mut().release(0);
    let min_column = agg_result.pin_mut().release(1);
    let max_column = agg_result.pin_mut().release(2);

    let keys = groupby_result.pin_mut().release_keys();
    assert!(keys.num_rows() > 0);

    // Verify the columns have the same size as the keys
    assert_eq!(sum_column.size(), keys.num_rows());
    assert_eq!(min_column.size(), keys.num_rows());
    assert_eq!(max_column.size(), keys.num_rows());

    Ok(())
}

#[test]
fn test_groupby_count() -> Result<(), Box<dyn std::error::Error>> {
    let table = ffi::read_parquet("../testdata/weather/result-000002.parquet")?;
    let table_view = table.view();

    let keys_view = table_view.select(&[0]);
    let groupby = ffi::groupby_create(&keys_view);

    let value_column = table_view.column(1);
    let mut agg_request = ffi::aggregation_request_create(&value_column);
    agg_request
        .pin_mut()
        .add(ffi::make_count_aggregation_groupby());

    let agg_requests = &[&*agg_request as *const ffi::AggregationRequest];
    let mut groupby_result = groupby.aggregate(agg_requests)?;

    let keys = groupby_result.pin_mut().release_keys();
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
