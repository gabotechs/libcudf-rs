mod pretty;

use arrow_schema::DataType;
use insta::assert_snapshot;
use libcudf_sys::{ffi, BinaryOperator, TypeId};
use pretty::pretty_column;

#[test]
fn test_binary_op_col_col_add() -> Result<(), Box<dyn std::error::Error>> {
    // Read a parquet file to get some test data
    let table = ffi::read_parquet("../testdata/weather/result-000000.parquet")?;
    let table_view = table.view();

    // Get two numeric columns
    let col1 = table_view.column(1);
    let col2 = table_view.column(2);

    // Add the two columns together
    let result = ffi::binary_operation_col_col(
        &col1,
        &col2,
        BinaryOperator::Add as i32,
        TypeId::Float64 as i32,
    )?;

    // Verify the result has the same size as the input columns
    assert_eq!(result.size(), col1.size());
    assert_eq!(result.size(), col2.size());
    assert_snapshot!(pretty_column(&result.view(), DataType::Float64)?);

    Ok(())
}

#[test]
fn test_binary_op_col_col_multiply() -> Result<(), Box<dyn std::error::Error>> {
    // Read a parquet file to get some test data
    let table = ffi::read_parquet("../testdata/weather/result-000000.parquet")?;
    let table_view = table.view();

    // Get two numeric columns
    let col1 = table_view.column(1);
    let col2 = table_view.column(2);

    // Multiply the two columns
    let result = ffi::binary_operation_col_col(
        &col1,
        &col2,
        BinaryOperator::Mul as i32,
        TypeId::Float64 as i32,
    )?;

    // Verify the result has the same size as the input columns
    assert_eq!(result.size(), col1.size());
    assert_snapshot!(pretty_column(&result.view(), DataType::Float64)?);

    Ok(())
}

#[test]
fn test_binary_operators_enum() {
    // Test that the enum values match cuDF's binary_operator values
    assert_eq!(BinaryOperator::Add as i32, 0);
    assert_eq!(BinaryOperator::Sub as i32, 1);
    assert_eq!(BinaryOperator::Mul as i32, 2);
    assert_eq!(BinaryOperator::Div as i32, 3);
}

#[test]
fn test_type_id_enum() {
    // Test that the enum values match cuDF's type_id values
    assert_eq!(TypeId::Int8 as i32, 1);
    assert_eq!(TypeId::Int32 as i32, 3);
    assert_eq!(TypeId::Float32 as i32, 9);
    assert_eq!(TypeId::Float64 as i32, 10);
}

#[test]
fn test_all_binary_operators() -> Result<(), Box<dyn std::error::Error>> {
    // Read a parquet file to get some test data
    let table = ffi::read_parquet("../testdata/weather/result-000000.parquet")?;
    let table_view = table.view();

    // Get two numeric columns
    let col1 = table_view.column(1);
    let col2 = table_view.column(2);

    // Test addition
    let result_add = ffi::binary_operation_col_col(
        &col1,
        &col2,
        BinaryOperator::Add as i32,
        TypeId::Float64 as i32,
    )?;
    assert_eq!(result_add.size(), col1.size());
    assert_snapshot!(pretty_column(&result_add.view(), DataType::Float64)?);

    // Test subtraction
    let result_sub = ffi::binary_operation_col_col(
        &col1,
        &col2,
        BinaryOperator::Sub as i32,
        TypeId::Float64 as i32,
    )?;
    assert_eq!(result_sub.size(), col1.size());
    assert_snapshot!(pretty_column(&result_sub.view(), DataType::Float64)?);

    // Test multiplication
    let result_mul = ffi::binary_operation_col_col(
        &col1,
        &col2,
        BinaryOperator::Mul as i32,
        TypeId::Float64 as i32,
    )?;
    assert_eq!(result_mul.size(), col1.size());
    assert_snapshot!(pretty_column(&result_mul.view(), DataType::Float64)?);

    // Test division
    let result_div = ffi::binary_operation_col_col(
        &col1,
        &col2,
        BinaryOperator::Div as i32,
        TypeId::Float64 as i32,
    )?;
    assert_eq!(result_div.size(), col1.size());
    assert_snapshot!(pretty_column(&result_div.view(), DataType::Float64)?);

    Ok(())
}
