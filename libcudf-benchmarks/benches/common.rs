use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

pub const SIZES: [usize; 3] = [10_000, 100_000, 1_000_000];

/// Create a RecordBatch for sorting/filtering benchmarks
/// Columns: id (Int64, shuffled), value (Float64), category (String), timestamp
pub fn create_numeric_batch(num_rows: usize) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]);

    // Shuffled integers for realistic sort benchmarks
    let int64_data: Vec<i64> = (0..num_rows as i64)
        .map(|x| (x.wrapping_mul(31337)) % (num_rows as i64))
        .collect();
    let float64_data: Vec<f64> = (0..num_rows)
        .map(|x| ((x * 17) % 1000) as f64 / 10.0)
        .collect();
    // 100 unique categories
    let string_data: Vec<String> = (0..num_rows).map(|x| format!("cat_{}", x % 100)).collect();
    let timestamp_data: Vec<i64> = (0..num_rows as i64)
        .map(|x| 1609459200000 + x * 1000)
        .collect();

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(int64_data)),
            Arc::new(Float64Array::from(float64_data)),
            Arc::new(StringArray::from(string_data)),
            Arc::new(TimestampMillisecondArray::from(timestamp_data)),
        ],
    )
    .unwrap()
}

/// Create a batch for group-by aggregations with controlled cardinality
pub fn create_groupby_batch(num_rows: usize, num_groups: usize) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("group_key", DataType::Int32, false),
        Field::new("value1", DataType::Int64, false),
        Field::new("value2", DataType::Float64, false),
    ]);

    let group_data: Vec<i32> = (0..num_rows).map(|x| (x % num_groups) as i32).collect();
    let value1_data: Vec<i64> = (0..num_rows as i64).collect();
    let value2_data: Vec<f64> = (0..num_rows).map(|x| x as f64 * 1.5).collect();

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int32Array::from(group_data)),
            Arc::new(Int64Array::from(value1_data)),
            Arc::new(Float64Array::from(value2_data)),
        ],
    )
    .unwrap()
}

/// Create a boolean mask with approximately `selectivity` fraction of true values
pub fn create_boolean_mask(num_rows: usize, selectivity: f64) -> BooleanArray {
    let threshold = (selectivity * 100.0) as usize;
    let data: Vec<bool> = (0..num_rows).map(|x| (x * 37) % 100 < threshold).collect();
    BooleanArray::from(data)
}
