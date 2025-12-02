#[cfg(test)]
mod tests {
    use arrow::array::{Float64Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use libcudf_rs::CuDFTable;
    use std::sync::Arc;

    #[test]
    fn test_arrow_roundtrip_simple() {
        use arrow::array::*;
        use arrow::datatypes::*;

        // Create a comprehensive Arrow RecordBatch with all major types
        let schema = Schema::new(vec![
            // Integer types - signed
            Field::new("int8", DataType::Int8, false),
            Field::new("int16", DataType::Int16, false),
            Field::new("int32", DataType::Int32, false),
            Field::new("int64", DataType::Int64, false),
            // Integer types - unsigned
            Field::new("uint8", DataType::UInt8, false),
            Field::new("uint16", DataType::UInt16, false),
            Field::new("uint32", DataType::UInt32, false),
            Field::new("uint64", DataType::UInt64, false),
            // Floating point
            Field::new("float32", DataType::Float32, false),
            Field::new("float64", DataType::Float64, false),
            // Boolean
            Field::new("bool", DataType::Boolean, false),
            // String
            Field::new("string", DataType::Utf8, false),
            // Date and Time (basic types)
            Field::new("date32", DataType::Date32, false),
            Field::new(
                "timestamp_ms",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]);

        let arrays: Vec<Arc<dyn arrow::array::Array>> = vec![
            // Signed integers
            Arc::new(Int8Array::from(vec![1i8, 2, 3, 4, 5])),
            Arc::new(Int16Array::from(vec![10i16, 20, 30, 40, 50])),
            Arc::new(Int32Array::from(vec![100i32, 200, 300, 400, 500])),
            Arc::new(Int64Array::from(vec![1000i64, 2000, 3000, 4000, 5000])),
            // Unsigned integers
            Arc::new(UInt8Array::from(vec![1u8, 2, 3, 4, 5])),
            Arc::new(UInt16Array::from(vec![10u16, 20, 30, 40, 50])),
            Arc::new(UInt32Array::from(vec![100u32, 200, 300, 400, 500])),
            Arc::new(UInt64Array::from(vec![1000u64, 2000, 3000, 4000, 5000])),
            // Floating point
            Arc::new(Float32Array::from(vec![1.5f32, 2.5, 3.5, 4.5, 5.5])),
            Arc::new(Float64Array::from(vec![10.5f64, 20.5, 30.5, 40.5, 50.5])),
            // Boolean
            Arc::new(BooleanArray::from(vec![true, false, true, false, true])),
            // String
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            // Date32 (days since epoch)
            Arc::new(Date32Array::from(vec![18000, 18001, 18002, 18003, 18004])),
            // Timestamp (milliseconds since epoch)
            Arc::new(TimestampMillisecondArray::from(vec![
                1609459200000i64, // 2021-01-01
                1609545600000,
                1609632000000,
                1609718400000,
                1609804800000,
            ])),
        ];

        let batch =
            RecordBatch::try_new(Arc::new(schema), arrays).expect("Failed to create RecordBatch");

        // Convert to cuDF Table
        let table = CuDFTable::from_arrow(batch.clone()).expect("Failed to convert to cuDF");

        // Verify dimensions
        assert_eq!(table.num_rows(), 5);
        assert_eq!(table.num_columns(), 14);

        // Convert back to Arrow
        let result_batch = table.to_arrow().expect("Failed to convert back to Arrow");

        // Verify schema - Note: column names are currently not preserved through cuDF
        // This is because cuDF tables store only data, not metadata like column names
        assert_eq!(result_batch.num_rows(), batch.num_rows());
        assert_eq!(result_batch.num_columns(), batch.num_columns());

        // Verify data types match (column names are not preserved yet)
        for (i, (original_field, result_field)) in batch
            .schema()
            .fields()
            .iter()
            .zip(result_batch.schema().fields().iter())
            .enumerate()
        {
            assert_eq!(
                result_field.data_type(),
                original_field.data_type(),
                "Data type mismatch for column {}: expected {:?}, got {:?}",
                i,
                original_field.data_type(),
                result_field.data_type()
            );
        }

        // Verify actual data values match for each column using Arrow's built-in equality
        for col_idx in 0..batch.num_columns() {
            let original_col = batch.column(col_idx);
            let result_col = result_batch.column(col_idx);

            assert_eq!(
                original_col,
                result_col,
                "Data mismatch for column {} (type: {:?})",
                col_idx,
                original_col.data_type()
            );
        }
    }

    #[test]
    fn test_arrow_empty_roundtrip() {
        // Create an empty RecordBatch
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Float64, false),
        ]);

        let id_array = Int32Array::from(Vec::<i32>::new());
        let value_array = Float64Array::from(Vec::<f64>::new());

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(id_array), Arc::new(value_array)],
        )
        .expect("Failed to create RecordBatch");

        // Convert to cuDF and back
        let table = CuDFTable::from_arrow(batch).expect("Failed to convert to cuDF");
        assert_eq!(table.num_rows(), 0);
        assert_eq!(table.num_columns(), 2);

        let result_batch = table.to_arrow().expect("Failed to convert back to Arrow");
        assert_eq!(result_batch.num_rows(), 0);
        assert_eq!(result_batch.num_columns(), 2);
    }

    #[test]
    fn test_arrow_parquet_roundtrip() {
        // Load from Parquet, convert to Arrow, filter, convert back
        let table = CuDFTable::from_parquet("testdata/weather/result-000000.parquet")
            .expect("Failed to read parquet");

        // Convert to Arrow
        let batch = table.to_arrow().expect("Failed to convert to Arrow");

        let original_rows = batch.num_rows();
        let original_cols = batch.num_columns();

        // Convert back to cuDF
        let table2 = CuDFTable::from_arrow(batch).expect("Failed to convert from Arrow");

        // Verify dimensions are preserved
        assert_eq!(table2.num_rows(), original_rows);
        assert_eq!(table2.num_columns(), original_cols);
    }
}
