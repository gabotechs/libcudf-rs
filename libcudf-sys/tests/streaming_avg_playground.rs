use crate::pretty::{pretty_column, pretty_table};
use arrow_schema::DataType;
use cxx::UniquePtr;
use libcudf_sys::{ffi, BinaryOperator, TypeId};
use libcudf_sys::ffi::TableView;
use std::error::Error;

mod pretty;

#[test]
fn concat() -> Result<(), Box<dyn Error>> {
    let table0 = ffi::read_parquet("../testdata/weather/result-000000.parquet")?;
    let table1 = ffi::read_parquet("../testdata/weather/result-000001.parquet")?;
    let table2 = ffi::read_parquet("../testdata/weather/result-000002.parquet")?;

    let concat = ffi::concat_table_views(&[table0.view(), table1.view(), table2.view()]);

    assert_eq!(
        concat.num_rows(),
        table0.num_rows() + table1.num_rows() + table2.num_rows()
    );

    Ok(())
}

#[test]
fn streaming_avg() -> Result<(), Box<dyn Error>> {
    let table0 = ffi::read_parquet("../testdata/weather/result-000000.parquet")?;
    let table1 = ffi::read_parquet("../testdata/weather/result-000001.parquet")?;
    let table2 = ffi::read_parquet("../testdata/weather/result-000002.parquet")?;

    fn partial(table_view: &TableView) -> Result<UniquePtr<ffi::GroupByResult>, cxx::Exception> {
        let group_by = ffi::groupby_create(&table_view.select(&[21]));

        let value_column = table_view.column(1);

        let mut request = ffi::aggregation_request_create(&value_column);

        request.pin_mut().add(ffi::make_sum_aggregation_groupby());
        request.pin_mut().add(ffi::make_count_aggregation_groupby());

        let agg_requests = &[&*request as *const ffi::AggregationRequest];
        group_by.aggregate(agg_requests)
    }

    let views = [table0.view(), table1.view(), table2.view()];

    let partials = views
        .iter()
        .map(|table| partial(table))
        .collect::<Result<Vec<_>, _>>()?;

    let keys = partials
        .iter()
        .map(|result| result.get_keys().view())
        .collect::<Vec<_>>();
    let keys = ffi::concat_table_views(&keys);

    let values = partials
        .iter()
        .map(|result| result.get_result(0))
        .collect::<Vec<_>>();

    let sums = values
        .iter()
        .map(|value| value.get(0).view())
        .collect::<Vec<_>>();
    let sums = ffi::concat_column_views(&sums);

    let counts = values
        .iter()
        .map(|value| value.get(1).view())
        .collect::<Vec<_>>();
    let counts = ffi::concat_column_views(&counts);

    // Final aggregation as a merge operation

    let group_by = ffi::groupby_create(&keys.view().select(&[0]));

    let mut sum_request = ffi::aggregation_request_create(&sums.view());
    sum_request
        .pin_mut()
        .add(ffi::make_sum_aggregation_groupby());

    let mut count_request = ffi::aggregation_request_create(&counts.view());
    count_request
        .pin_mut()
        .add(ffi::make_sum_aggregation_groupby());

    let agg_requests = &[
        &*sum_request as *const ffi::AggregationRequest,
        &*count_request as *const ffi::AggregationRequest,
    ];

    let result = group_by.aggregate(agg_requests)?;
    let keys = result.get_keys();
    let sum = result.get_result(0).get(0);
    let count = result.get_result(1).get(0);


    let avg = ffi::binary_operation_col_col(
        &sum.view(),
        &count.view(),
        BinaryOperator::Div as i32,
        TypeId::Float64 as i32,
    )?;
    
    eprintln!("{}", pretty_table(&result.get_keys().view())?);
    eprintln!("{}", pretty_column(&avg.view(), DataType::Float64)?);

    Ok(())
}
