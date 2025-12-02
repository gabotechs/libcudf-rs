use arrow::array::{Array, Int32Array, StringArray};
use libcudf_rs::CuDFColumn;

#[test]
fn test_column_from_arrow_int32() {
    let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let column = CuDFColumn::from_arrow(&array).expect("Failed to convert Arrow array to column");

    assert_eq!(column.len(), 5);
    assert!(!column.is_empty());
}

#[test]
fn test_column_from_arrow_string() {
    let array = StringArray::from(vec!["hello", "world", "test"]);
    let column =
        CuDFColumn::from_arrow(&array).expect("Failed to convert Arrow string array to column");

    assert_eq!(column.len(), 3);
    assert!(!column.is_empty());
}

#[test]
fn test_column_from_arrow_empty() {
    let array = Int32Array::from(Vec::<i32>::new());
    let column =
        CuDFColumn::from_arrow(&array).expect("Failed to convert empty Arrow array to column");

    assert_eq!(column.len(), 0);
    assert!(column.is_empty());
}

#[test]
fn test_column_from_arrow_with_nulls() {
    let array = Int32Array::from(vec![Some(1), None, Some(3), None, Some(5)]);
    let column =
        CuDFColumn::from_arrow(&array).expect("Failed to convert Arrow array with nulls to column");

    assert_eq!(column.len(), 5);
    assert!(!column.is_empty());
}
