#[cfg(test)]
mod tests {
    use arrow::array::{Array, Float64Array, Int32Array, Int64Array, StringArray};
    use libcudf_rs::{CuDFColumn, CuDFColumnView};

    #[test]
    fn test_column_from_arrow_int32() {
        let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let column = CuDFColumn::from_arrow_host(&array)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        assert_eq!(column.len(), 5);
        assert!(!column.is_empty());
    }

    #[test]
    fn test_column_from_arrow_string() {
        let array = StringArray::from(vec!["hello", "world", "test"]);
        let column = CuDFColumn::from_arrow_host(&array)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        assert_eq!(column.len(), 3);
        assert!(!column.is_empty());
    }

    #[test]
    fn test_column_from_arrow_empty() {
        let array = Int32Array::from(Vec::<i32>::new());
        let column = CuDFColumn::from_arrow_host(&array)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        assert_eq!(column.len(), 0);
        assert!(column.is_empty());
    }

    #[test]
    fn test_column_from_arrow_with_nulls() {
        let array = Int32Array::from(vec![Some(1), None, Some(3), None, Some(5)]);
        let column = CuDFColumn::from_arrow_host(&array)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        assert_eq!(column.len(), 5);
        assert!(!column.is_empty());
    }

    #[test]
    fn test_to_arrow_host_int32() {
        let original = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let column = CuDFColumn::from_arrow_host(&original)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        let result = column
            .to_arrow_host()
            .expect("Failed to convert column back to Arrow");

        assert_eq!(result.len(), 5);
        let result_int32 = result
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Failed to downcast to Int32Array");

        for i in 0..5 {
            assert_eq!(result_int32.value(i), original.value(i));
        }
    }

    #[test]
    fn test_to_arrow_host_int64() {
        let original = Int64Array::from(vec![100, 200, 300, 400, 500]);
        let column = CuDFColumn::from_arrow_host(&original)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        let result = column
            .to_arrow_host()
            .expect("Failed to convert column back to Arrow");

        assert_eq!(result.len(), 5);
        let result_int64 = result
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Failed to downcast to Int64Array");

        for i in 0..5 {
            assert_eq!(result_int64.value(i), original.value(i));
        }
    }

    #[test]
    fn test_to_arrow_host_float64() {
        let original = Float64Array::from(vec![1.5, 2.5, 3.5, 4.5, 5.5]);
        let column = CuDFColumn::from_arrow_host(&original)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        let result = column
            .to_arrow_host()
            .expect("Failed to convert column back to Arrow");

        assert_eq!(result.len(), 5);
        let result_float64 = result
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Failed to downcast to Float64Array");

        for i in 0..5 {
            assert_eq!(result_float64.value(i), original.value(i));
        }
    }

    #[test]
    fn test_to_arrow_host_string() {
        let original = StringArray::from(vec!["hello", "world", "test", "cudf", "rust"]);
        let column = CuDFColumn::from_arrow_host(&original)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        let result = column
            .to_arrow_host()
            .expect("Failed to convert column back to Arrow");

        assert_eq!(result.len(), 5);
        let result_string = result
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast to StringArray");

        for i in 0..5 {
            assert_eq!(result_string.value(i), original.value(i));
        }
    }

    #[test]
    fn test_to_arrow_host_with_nulls() {
        let original = Int32Array::from(vec![Some(1), None, Some(3), None, Some(5)]);
        let column = CuDFColumn::from_arrow_host(&original)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        let result = column
            .to_arrow_host()
            .expect("Failed to convert column back to Arrow");

        assert_eq!(result.len(), 5);
        let result_int32 = result
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Failed to downcast to Int32Array");

        assert!(result_int32.is_valid(0));
        assert_eq!(result_int32.value(0), 1);

        assert!(result_int32.is_null(1));

        assert!(result_int32.is_valid(2));
        assert_eq!(result_int32.value(2), 3);

        assert!(result_int32.is_null(3));

        assert!(result_int32.is_valid(4));
        assert_eq!(result_int32.value(4), 5);
    }

    #[test]
    fn test_to_arrow_host_empty() {
        let original = Int32Array::from(Vec::<i32>::new());
        let column = CuDFColumn::from_arrow_host(&original)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        let result = column
            .to_arrow_host()
            .expect("Failed to convert empty column back to Arrow");

        assert_eq!(result.len(), 0);
        assert!(result.is_empty());
    }

    #[test]
    fn test_to_arrow_host_roundtrip_preserves_data() {
        let original = Int32Array::from(vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100]);
        let column = CuDFColumn::from_arrow_host(&original)
            .expect("Failed to convert Arrow array to column")
            .into_view();

        let result = column
            .to_arrow_host()
            .expect("Failed to convert column back to Arrow");

        let result_int32 = result
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Failed to downcast to Int32Array");

        // Verify all values match
        assert_eq!(result_int32.len(), original.len());
        for i in 0..original.len() {
            assert_eq!(result_int32.value(i), original.value(i));
        }
    }
}
