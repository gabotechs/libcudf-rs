use crate::cudf_array::is_cudf_array;
use crate::table_view::CuDFTableView;
use crate::{CuDFColumn, CuDFError};
use arrow::array::{Array, ArrayData, StructArray};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use arrow_schema::ArrowError;
use cxx::UniquePtr;
use libcudf_sys::{ffi, ArrowDeviceArray};
use std::path::Path;
use std::sync::Arc;

/// A GPU-accelerated table (similar to a DataFrame)
///
/// This is a safe wrapper around cuDF's table type.
pub struct CuDFTable {
    pub(crate) inner: UniquePtr<ffi::Table>,
}

impl CuDFTable {
    /// Create a CuDFTable from a raw FFI table (internal use)
    pub(crate) fn from_inner(inner: UniquePtr<ffi::Table>) -> Self {
        Self { inner }
    }
    /// Create an empty table
    ///
    /// # Examples
    ///
    /// ```
    /// use libcudf_rs::CuDFTable;
    ///
    /// let table = CuDFTable::default();
    /// assert_eq!(table.num_rows(), 0);
    /// assert_eq!(table.num_columns(), 0);
    /// ```
    pub(crate) fn new() -> Self {
        Self {
            inner: ffi::create_empty_table(),
        }
    }

    pub(crate) fn from_ptr(inner: UniquePtr<ffi::Table>) -> Self {
        Self { inner }
    }

    /// Read a table from a Parquet file
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the Parquet file
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The file does not exist or cannot be read
    /// - The Parquet format is invalid
    /// - There is insufficient GPU memory
    /// - The path contains invalid UTF-8
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use libcudf_rs::CuDFTable;
    ///
    /// let table = CuDFTable::from_parquet("data.parquet")?;
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn from_parquet<P: AsRef<Path>>(path: P) -> Result<Self, CuDFError> {
        let path_str = path.as_ref().to_str().ok_or_else(|| {
            ArrowError::InvalidArgumentError("Path contains invalid UTF-8".to_string())
        })?;

        let inner = ffi::read_parquet(path_str)?;
        Ok(Self { inner })
    }

    /// Write the table to a Parquet file
    ///
    /// # Arguments
    ///
    /// * `path` - Path where the Parquet file will be written
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The file cannot be created or written
    /// - There is insufficient GPU memory
    /// - The path contains invalid UTF-8
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use libcudf_rs::CuDFTable;
    /// # let table: CuDFTable = todo!();
    /// table.to_parquet("output.parquet")?;
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn to_parquet<P: AsRef<Path>>(&self, path: P) -> Result<(), CuDFError> {
        let path_str = path.as_ref().to_str().ok_or_else(|| {
            ArrowError::InvalidArgumentError("Path contains invalid UTF-8".to_string())
        })?;

        let view = self.inner.view();
        ffi::write_parquet(&view, path_str)?;
        Ok(())
    }

    /// Create a table from an Arrow RecordBatch
    ///
    /// This enables seamless integration with arrow-rs, allowing you to use
    /// Arrow's rich ecosystem and then accelerate operations with cuDF on GPU.
    ///
    /// # Arguments
    ///
    /// * `batch` - An Arrow RecordBatch
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The Arrow data cannot be converted to cuDF format
    /// - The Arrow RecordBatch contains columns that are already in cuDF
    /// - There is insufficient GPU memory
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use arrow::record_batch::RecordBatch;
    /// use libcudf_rs::CuDFTable;
    ///
    /// # let batch: RecordBatch = todo!();
    /// // Convert Arrow RecordBatch to cuDF table for GPU acceleration
    /// let table = CuDFTable::from_arrow_host(batch)?;
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn from_arrow_host(batch: RecordBatch) -> Result<Self, CuDFError> {
        for col in batch.columns() {
            if is_cudf_array(col) {
                return Err(ArrowError::InvalidArgumentError("Tried to move a RecordBatch from the host to CuDF, but a column was already in CuDF".to_string()))?;
            }
        }
        let schema = batch.schema().as_ref().clone();
        let struct_array = StructArray::from(batch);
        let array_data: ArrayData = struct_array.into_data();

        let ffi_array = FFI_ArrowArray::new(&array_data);
        let ffi_schema = FFI_ArrowSchema::try_from(schema)?;

        let device_array = ArrowDeviceArray {
            array: ffi_array,
            device_id: -1,
            device_type: 1, // CPU
            sync_event: std::ptr::null_mut(),
            reserved: [0; 3],
        };

        let schema_ptr = &ffi_schema as *const FFI_ArrowSchema as *const u8;
        let device_array_ptr = &device_array as *const ArrowDeviceArray as *const u8;
        let inner = unsafe { ffi::table_from_arrow_host(schema_ptr, device_array_ptr) }?;

        Ok(Self { inner })
    }

    /// Get the number of rows in the table
    ///
    /// # Examples
    ///
    /// ```
    /// use libcudf_rs::CuDFTable;
    ///
    /// let table = CuDFTable::default();
    /// assert_eq!(table.num_rows(), 0);
    /// ```
    pub fn num_rows(&self) -> usize {
        self.inner.num_rows()
    }

    /// Get the number of columns in the table
    ///
    /// # Examples
    ///
    /// ```
    /// use libcudf_rs::CuDFTable;
    ///
    /// let table = CuDFTable::default();
    /// assert_eq!(table.num_columns(), 0);
    /// ```
    pub fn num_columns(&self) -> usize {
        self.inner.num_columns()
    }

    /// Check if the table is empty
    ///
    /// # Examples
    ///
    /// ```
    /// use libcudf_rs::CuDFTable;
    ///
    /// let table = CuDFTable::default();
    /// assert!(table.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }

    /// Get a non-owning view of this table
    ///
    /// The returned view borrows from this table and remains valid as long as
    /// the table exists.
    pub fn view(self: Arc<Self>) -> CuDFTableView {
        CuDFTableView::new_with_ref(self.inner.view(), Some(self))
    }

    /// Get a non-owning view of this table
    pub fn into_view(self) -> CuDFTableView {
        CuDFTableView::new_with_ref(self.inner.view(), Some(Arc::new(self)))
    }

    /// Take ownership of the table's columns
    ///
    /// This consumes the table structure and returns its columns as a collection
    /// that can be individually released.
    pub fn into_columns(mut self) -> Vec<CuDFColumn> {
        let mut columns = self.inner.pin_mut().release();
        let mut result = Vec::with_capacity(columns.len());
        for i in 0..columns.len() {
            let col = columns.pin_mut().release(i);
            result.push(CuDFColumn::new(col));
        }
        result
    }

    /// Concatenate multiple table views into a single table
    ///
    /// # Arguments
    ///
    /// * `views` - Slice of table views to concatenate (consumes the views)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The tables have incompatible schemas
    /// - There is insufficient GPU memory
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use libcudf_rs::CuDFTable;
    ///
    /// let table1 = CuDFTable::from_parquet("data1.parquet")?;
    /// let table2 = CuDFTable::from_parquet("data2.parquet")?;
    ///
    /// let views = vec![table1.into_view(), table2.into_view()];
    /// let concatenated = CuDFTable::concat(views)?;
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn concat(views: Vec<CuDFTableView>) -> Result<Self, CuDFError> {
        // The CuDFTableView need to leave at least until the ffi::concat_table_views operation
        // has finished.
        let mut _refs = Vec::with_capacity(views.len());
        let inner_views: Vec<_> = views
            .into_iter()
            .map(|v| {
                _refs.push(v._ref.clone());
                v.into_inner()
            })
            .collect();
        let inner = ffi::concat_table_views(&inner_views)?;
        Ok(Self { inner })
    }
}

impl Default for CuDFTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use arrow::datatypes::*;

    #[test]
    fn test_empty_table() {
        let table = CuDFTable::new();
        assert_eq!(table.num_rows(), 0);
        assert_eq!(table.num_columns(), 0);
        assert!(table.is_empty());
    }

    #[test]
    fn test_default_table() {
        let table = CuDFTable::default();
        assert!(table.is_empty());
    }

    #[test]
    fn test_arrow_roundtrip_simple() {
        let schema = Schema::new(vec![
            Field::new("int8", DataType::Int8, false),
            Field::new("int16", DataType::Int16, false),
            Field::new("int32", DataType::Int32, false),
            Field::new("int64", DataType::Int64, false),
            Field::new("uint8", DataType::UInt8, false),
            Field::new("uint16", DataType::UInt16, false),
            Field::new("uint32", DataType::UInt32, false),
            Field::new("uint64", DataType::UInt64, false),
            Field::new("float32", DataType::Float32, false),
            Field::new("float64", DataType::Float64, false),
            Field::new("bool", DataType::Boolean, false),
            Field::new("string", DataType::Utf8, false),
            Field::new("date32", DataType::Date32, false),
            Field::new(
                "timestamp_ms",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]);

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(Int8Array::from(vec![1i8, 2, 3, 4, 5])),
            Arc::new(Int16Array::from(vec![10i16, 20, 30, 40, 50])),
            Arc::new(Int32Array::from(vec![100i32, 200, 300, 400, 500])),
            Arc::new(Int64Array::from(vec![1000i64, 2000, 3000, 4000, 5000])),
            Arc::new(UInt8Array::from(vec![1u8, 2, 3, 4, 5])),
            Arc::new(UInt16Array::from(vec![10u16, 20, 30, 40, 50])),
            Arc::new(UInt32Array::from(vec![100u32, 200, 300, 400, 500])),
            Arc::new(UInt64Array::from(vec![1000u64, 2000, 3000, 4000, 5000])),
            Arc::new(Float32Array::from(vec![1.5f32, 2.5, 3.5, 4.5, 5.5])),
            Arc::new(Float64Array::from(vec![10.5f64, 20.5, 30.5, 40.5, 50.5])),
            Arc::new(BooleanArray::from(vec![true, false, true, false, true])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            Arc::new(Date32Array::from(vec![18000, 18001, 18002, 18003, 18004])),
            Arc::new(TimestampMillisecondArray::from(vec![
                1609459200000i64,
                1609545600000,
                1609632000000,
                1609718400000,
                1609804800000,
            ])),
        ];

        let batch =
            RecordBatch::try_new(Arc::new(schema), arrays).expect("Failed to create RecordBatch");

        let table = CuDFTable::from_arrow_host(batch.clone()).expect("Failed to convert to cuDF");

        assert_eq!(table.num_rows(), 5);
        assert_eq!(table.num_columns(), 14);

        let result_batch = table
            .into_view()
            .to_arrow_host()
            .expect("Failed to convert back to Arrow");

        assert_eq!(result_batch.num_rows(), batch.num_rows());
        assert_eq!(result_batch.num_columns(), batch.num_columns());

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

        let table = CuDFTable::from_arrow_host(batch).expect("Failed to convert to cuDF");
        assert_eq!(table.num_rows(), 0);
        assert_eq!(table.num_columns(), 2);

        let result_batch = table
            .into_view()
            .to_arrow_host()
            .expect("Failed to convert back to Arrow");
        assert_eq!(result_batch.num_rows(), 0);
        assert_eq!(result_batch.num_columns(), 2);
    }

    #[test]
    fn test_arrow_parquet_roundtrip() {
        let table = CuDFTable::from_parquet("testdata/weather/result-000000.parquet")
            .expect("Failed to read parquet");

        let batch = table
            .into_view()
            .to_arrow_host()
            .expect("Failed to convert to Arrow");

        let original_rows = batch.num_rows();
        let original_cols = batch.num_columns();

        let table2 = CuDFTable::from_arrow_host(batch).expect("Failed to convert from Arrow");

        assert_eq!(table2.num_rows(), original_rows);
        assert_eq!(table2.num_columns(), original_cols);
    }

    #[test]
    fn test_read_all_weather_files() {
        for i in 0..3 {
            let filename = format!("testdata/weather/result-{:06}.parquet", i);
            let table = CuDFTable::from_parquet(&filename)
                .unwrap_or_else(|_| panic!("Failed to read {}", filename));

            assert!(table.num_rows() > 0);
            assert!(table.num_columns() > 0);
        }
    }
}
