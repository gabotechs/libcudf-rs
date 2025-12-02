use std::error::Error;
use libcudf_sys::ffi;

mod pretty;

#[test]
fn concat() -> Result<(), Box<dyn Error>> {
    let table0 = ffi::read_parquet("../testdata/weather/result-000000.parquet")?;
    let table1 = ffi::read_parquet("../testdata/weather/result-000001.parquet")?;
    let table2 = ffi::read_parquet("../testdata/weather/result-000002.parquet")?;

    let concat = ffi::concat(&[table0.view(), table1.view(), table2.view()]);

    assert_eq!(concat.num_rows(), table0.num_rows() + table1.num_rows() + table2.num_rows());

    Ok(())
}

#[test]
fn streaming_avg() -> Result<(), Box<dyn Error>> {
    // TODO
    Ok(())
}
