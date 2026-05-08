use crate::data_type::cudf_type_to_arrow;
use crate::{CuDFColumn, CuDFError};
use arrow::array::{Array, ArrayData, ArrayRef, Scalar};
use arrow::buffer::NullBuffer;
use arrow_schema::DataType;
use cxx::UniquePtr;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, RwLock};

/// A single value stored in GPU memory
///
/// CuDFScalar wraps a cuDF scalar and implements Arrow's Array trait with length 1.
/// This allows scalar values to be used in contexts that expect arrays, enabling
/// seamless integration with operations that work on both scalars and columns.
pub struct CuDFScalar {
    inner: UniquePtr<libcudf_sys::ffi::Scalar>,
    dt: DataType,
    cached_scalar: RwLock<Option<Arc<ArrayData>>>,
}

impl CuDFScalar {
    /// Create a CuDFScalar from an existing cuDF scalar
    pub(crate) fn new(inner: UniquePtr<libcudf_sys::ffi::Scalar>) -> Self {
        let cudf_dtype = inner.data_type();
        let dt = cudf_type_to_arrow(&cudf_dtype);
        let dt = dt.unwrap_or(DataType::Null);
        let cached_scalar = RwLock::new(None);
        Self {
            inner,
            dt,
            cached_scalar,
        }
    }

    /// Get a reference to the underlying cuDF scalar
    pub(crate) fn inner(&self) -> &UniquePtr<libcudf_sys::ffi::Scalar> {
        &self.inner
    }

    /// Convert the cuDF scalar to a single-element Arrow array, copying data from GPU to host
    ///
    /// This copies the GPU data back to the CPU and creates an Arrow array containing a single
    /// element.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The cuDF scalar cannot be converted to Arrow format
    /// - There is an error copying data from GPU to host
    /// - There is insufficient memory for the conversion
    ///
    /// # Example
    ///
    /// ```no_run
    /// use arrow::array::{Array, Int32Array, Scalar};
    /// use libcudf_rs::CuDFScalar;
    ///
    /// // Create a cuDF scalar from an Arrow scalar
    /// let array = Int32Array::from(vec![42]);
    /// let scalar = Scalar::new(&array);
    /// let cudf_scalar = CuDFScalar::from_arrow_host(scalar)?;
    ///
    /// // Convert back to Arrow (GPU -> CPU)
    /// let arrow_array = cudf_scalar.to_arrow_host()?;
    ///
    /// // Verify the result
    /// assert_eq!(arrow_array.len(), 1);
    /// assert_eq!(arrow_array.null_count(), 0);
    ///
    /// // Downcast to specific type and get value
    /// let int32_array = arrow_array
    ///     .as_any()
    ///     .downcast_ref::<Int32Array>()
    ///     .expect("Expected Int32Array");
    /// assert_eq!(int32_array.value(0), 42);
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn to_arrow_host(&self) -> Result<ArrayRef, CuDFError> {
        let mut device_array = libcudf_sys::ArrowDeviceArray::new_cpu();

        // Convert the scalar to Arrow format (copying from GPU to host)
        // cuDF's to_arrow_array expects an ArrowDeviceArray pointer
        let stream = crate::stream::default_stream();
        let mr = crate::stream::current_device_resource();
        unsafe {
            let device_array_ptr =
                &mut device_array as *mut libcudf_sys::ArrowDeviceArray as *mut u8;
            self.inner.to_arrow_array(
                device_array_ptr,
                crate::stream::stream_ref(&stream),
                crate::stream::resource_ref(&mr),
            );
        }

        // Convert from FFI structures to Arrow ArrayData
        // Extract just the ArrowArray part
        let array_data =
            unsafe { arrow::ffi::from_ffi_and_data_type(device_array.array, self.dt.clone())? };

        // Create an ArrayRef from the ArrayData
        Ok(arrow::array::make_array(array_data))
    }

    /// Convert an Arrow scalar to a cuDF scalar
    ///
    /// This creates a single-element column from the scalar, transfers it to GPU,
    /// and extracts the scalar from that column.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The Arrow scalar cannot be converted to cuDF format
    /// - There is insufficient GPU memory
    ///
    /// # Example
    ///
    /// ```no_run
    /// use arrow::array::{Int32Array, Scalar};
    /// use libcudf_rs::CuDFScalar;
    ///
    /// let array = Int32Array::from(vec![42]);
    /// let scalar = Scalar::new(&array);
    /// let cudf_scalar = CuDFScalar::from_arrow_host(scalar)?;
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn from_arrow_host<T: Array>(scalar: Scalar<T>) -> Result<Self, CuDFError> {
        // Convert scalar to a single-element array
        let array = scalar.into_inner();

        // Convert the array to a cuDF column (this copies to GPU)
        let column = CuDFColumn::from_arrow_host(&array)?.into_view();

        // Extract the scalar from the column at index 0
        let stream = crate::stream::default_stream();
        let mr = crate::stream::current_device_resource();
        let cudf_scalar = libcudf_sys::ffi::get_element(
            column.inner(),
            0,
            crate::stream::stream_ref(&stream),
            crate::stream::resource_ref(&mr),
        );

        Ok(Self::new(cudf_scalar))
    }

    /// Get or compute the cached ArrayData for this scalar
    ///
    /// This method lazily converts the GPU scalar to CPU ArrayData and caches the results. This
    /// avoids subsequent calls performing more expensive GPU->CPU transfer by returning the cached
    /// data.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The scalar cannot be converted to Arrow format
    /// - There is insufficient memory for the conversion
    ///
    /// # Example
    ///
    /// ```no_run
    /// use arrow::array::{Int32Array, Scalar};
    /// use libcudf_rs::CuDFScalar;
    ///
    /// let array = Int32Array::from(vec![42]);
    /// let scalar = Scalar::new(&array);
    /// let cudf_scalar = CuDFScalar::from_arrow_host(scalar)?;
    ///
    /// // First call: converts GPU -> CPU and caches
    /// let data1 = cudf_scalar.get_scalar_data()?;
    ///
    /// // Second call: returns cached data
    /// let data2 = cudf_scalar.get_scalar_data()?;
    ///
    /// assert_eq!(data1.len(), 1);
    /// assert_eq!(data2.len(), 1);
    /// # Ok::<(), libcudf_rs::CuDFError>(())
    /// ```
    pub fn get_scalar_data(&self) -> Result<Arc<ArrayData>, CuDFError> {
        if let Ok(cache) = self.cached_scalar.read() {
            if let Some(cached_data) = cache.as_ref() {
                return Ok(Arc::clone(cached_data));
            }
        }

        let array_ref = self.to_arrow_host()?;
        let array_data = Arc::new(array_ref.to_data());
        if let Ok(mut cache) = self.cached_scalar.write() {
            *cache = Some(Arc::clone(&array_data));
        }

        Ok(array_data)
    }
}

impl Debug for CuDFScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CuDFScalar: type={}", self.dt)
    }
}

impl Clone for CuDFScalar {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            dt: self.dt.clone(),
            cached_scalar: RwLock::new(None),
        }
    }
}

unsafe impl Array for CuDFScalar {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_data(&self) -> ArrayData {
        // WARNING: This performs a full GPU to CPU data transfer.
        self.to_arrow_host()
            .expect("Failed to convert GPU scalar to host Arrow")
            .to_data()
    }

    fn into_data(self) -> ArrayData {
        self.to_data()
    }

    fn data_type(&self) -> &DataType {
        &self.dt
    }

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        if offset >= 1 || length == 0 {
            arrow::array::new_empty_array(&self.dt)
        } else {
            Arc::new(Self {
                inner: self.inner.clone(),
                dt: self.dt.clone(),
                cached_scalar: RwLock::new(None),
            })
        }
    }

    fn len(&self) -> usize {
        1
    }

    fn is_empty(&self) -> bool {
        false
    }

    fn offset(&self) -> usize {
        0
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        None
    }

    fn get_buffer_memory_size(&self) -> usize {
        self.get_scalar_data()
            .map(|data| data.get_buffer_memory_size())
            .unwrap_or(0)
    }

    fn get_array_memory_size(&self) -> usize {
        // Array memory = buffer memory + null bitmap
        self.get_scalar_data()
            .map(|data| data.get_array_memory_size())
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;

    /// Assert Array trait basic properties
    fn assert_array_trait_basics(scalar: &CuDFScalar, expected_type: &DataType) {
        assert_eq!(scalar.len(), 1, "Scalar length should always be 1");
        assert!(!scalar.is_empty(), "Scalar should never be empty");
        assert_eq!(scalar.offset(), 0, "Scalar offset should always be 0");
        assert_eq!(scalar.data_type(), expected_type, "Data type mismatch");
        assert!(
            scalar.nulls().is_none(),
            "nulls() should return None for scalars"
        );
    }

    /// Test comprehensive slice behavior for a scalar
    fn assert_slice_behavior(scalar: &CuDFScalar) {
        let dt = scalar.data_type();

        // slice(0, 1) -> returns self
        let sliced = scalar.slice(0, 1);
        assert_eq!(sliced.len(), 1, "slice(0, 1) should return self");
        assert_eq!(sliced.data_type(), dt, "slice(0, 1) should preserve type");

        // slice(0, 0) -> returns empty
        let empty = scalar.slice(0, 0);
        assert_eq!(empty.len(), 0, "slice(0, 0) should return empty array");
        assert_eq!(empty.data_type(), dt, "Empty slice should preserve type");

        // slice(1, 1) -> returns empty (offset beyond bounds)
        let oob = scalar.slice(1, 1);
        assert_eq!(oob.len(), 0, "slice(1, 1) should return empty array");

        // slice(5, 10) -> returns empty (way out of bounds)
        let far_oob = scalar.slice(5, 10);
        assert_eq!(far_oob.len(), 0, "slice(5, 10) should return empty array");

        // slice(0, 100) -> returns self (length clamped)
        let oversized = scalar.slice(0, 100);
        assert_eq!(
            oversized.len(),
            1,
            "slice(0, 100) should return self (clamped)"
        );
    }

    /// Verify slice clears cache
    fn assert_slice_clears_cache(scalar: &CuDFScalar) {
        // First call to populate cache
        let _ = scalar.get_buffer_memory_size();

        // Slice creates new instance
        let sliced = scalar.slice(0, 1);

        // Both scalars should still work independently
        assert!(scalar.get_buffer_memory_size() > 0);
        assert!(sliced.get_buffer_memory_size() > 0);
    }

    /// Assert memory size for a scalar
    fn assert_memory_size(scalar: &CuDFScalar, expected_buffer_size: usize, type_name: &str) {
        let buffer_size = scalar.get_buffer_memory_size();
        let array_size = scalar.get_array_memory_size();

        assert_eq!(
            buffer_size, expected_buffer_size,
            "Buffer size mismatch for {}",
            type_name
        );

        // Array size should include null bitmap overhead
        assert!(
            array_size >= buffer_size,
            "Array size ({}) should be >= buffer size ({})",
            array_size,
            buffer_size
        );
    }

    #[test]
    fn test_to_arrow_host_int32() -> Result<(), Box<dyn std::error::Error>> {
        let array = Int32Array::from(vec![42]);
        let cudf_scalar = CuDFScalar::from_arrow_host(Scalar::new(&array))?;
        let result = cudf_scalar.to_arrow_host()?;

        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), &DataType::Int32);
        let result_array = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(result_array.value(0), 42);

        Ok(())
    }

    #[test]
    fn test_to_arrow_host_int64() -> Result<(), Box<dyn std::error::Error>> {
        let array = Int64Array::from(vec![12345_i64]);
        let cudf_scalar = CuDFScalar::from_arrow_host(Scalar::new(&array))?;
        let result = cudf_scalar.to_arrow_host()?;

        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), &DataType::Int64);
        let result_array = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(result_array.value(0), 12345);

        Ok(())
    }

    #[test]
    fn test_to_arrow_host_float64() -> Result<(), Box<dyn std::error::Error>> {
        let array = Float64Array::from(vec![std::f64::consts::PI]);
        let cudf_scalar = CuDFScalar::from_arrow_host(Scalar::new(&array))?;
        let result = cudf_scalar.to_arrow_host()?;

        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), &DataType::Float64);
        let result_array = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(result_array.value(0), std::f64::consts::PI);

        Ok(())
    }

    #[test]
    fn test_to_arrow_host_boolean() -> Result<(), Box<dyn std::error::Error>> {
        let array = BooleanArray::from(vec![true]);
        let cudf_scalar = CuDFScalar::from_arrow_host(Scalar::new(&array))?;
        let result = cudf_scalar.to_arrow_host()?;

        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), &DataType::Boolean);
        let result_array = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(result_array.value(0));

        Ok(())
    }

    #[test]
    fn test_to_arrow_host_string() -> Result<(), Box<dyn std::error::Error>> {
        let array = StringArray::from(vec!["hello world"]);
        let cudf_scalar = CuDFScalar::from_arrow_host(Scalar::new(&array))?;
        let result = cudf_scalar.to_arrow_host()?;

        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), &DataType::Utf8);
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "hello world");

        Ok(())
    }

    #[test]
    fn test_array_trait_slice_behavior_int32() -> Result<(), Box<dyn std::error::Error>> {
        let scalar = CuDFScalar::from_arrow_host(Scalar::new(&Int32Array::from(vec![42])))
            .expect("Failed to create scalar");
        assert_slice_behavior(&scalar);

        Ok(())
    }

    #[test]
    fn test_array_trait_slice_behavior_float64() -> Result<(), Box<dyn std::error::Error>> {
        let scalar = CuDFScalar::from_arrow_host(Scalar::new(&Float64Array::from(vec![
            std::f64::consts::PI,
        ])))
        .expect("Failed to create scalar");
        assert_slice_behavior(&scalar);

        Ok(())
    }

    #[test]
    fn test_array_trait_slice_behavior_string() -> Result<(), Box<dyn std::error::Error>> {
        let scalar = CuDFScalar::from_arrow_host(Scalar::new(&StringArray::from(vec!["test"])))
            .expect("Failed to create scalar");
        assert_slice_behavior(&scalar);

        Ok(())
    }

    #[test]
    fn test_array_trait_slice_behavior_null() -> Result<(), Box<dyn std::error::Error>> {
        let scalar = CuDFScalar::from_arrow_host(Scalar::new(&Int32Array::from(vec![None])))
            .expect("Failed to create scalar");
        assert_slice_behavior(&scalar);

        Ok(())
    }

    #[test]
    fn test_array_trait_slice_clears_cache() -> Result<(), Box<dyn std::error::Error>> {
        let scalar = CuDFScalar::from_arrow_host(Scalar::new(&Int64Array::from(vec![999])))
            .expect("Failed to create scalar");
        assert_slice_clears_cache(&scalar);

        Ok(())
    }

    #[test]
    fn test_array_trait_basics_int32() -> Result<(), Box<dyn std::error::Error>> {
        let scalar = CuDFScalar::from_arrow_host(Scalar::new(&Int32Array::from(vec![42])))
            .expect("Failed to create scalar");
        assert_array_trait_basics(&scalar, &DataType::Int32);

        Ok(())
    }

    #[test]
    fn test_array_trait_basics_string() -> Result<(), Box<dyn std::error::Error>> {
        let scalar = CuDFScalar::from_arrow_host(Scalar::new(&StringArray::from(vec!["test"])))
            .expect("Failed to create scalar");
        assert_array_trait_basics(&scalar, &DataType::Utf8);

        Ok(())
    }

    #[test]
    fn test_array_trait_basics_boolean() -> Result<(), Box<dyn std::error::Error>> {
        let scalar = CuDFScalar::from_arrow_host(Scalar::new(&BooleanArray::from(vec![true])))
            .expect("Failed to create scalar");
        assert_array_trait_basics(&scalar, &DataType::Boolean);

        Ok(())
    }

    #[test]
    fn test_array_trait_basics_null() -> Result<(), Box<dyn std::error::Error>> {
        let scalar = CuDFScalar::from_arrow_host(Scalar::new(&Int32Array::from(vec![None])))
            .expect("Failed to create scalar");
        assert_array_trait_basics(&scalar, &DataType::Int32);

        Ok(())
    }

    #[test]
    fn test_as_any_downcast() -> Result<(), Box<dyn std::error::Error>> {
        let scalar = CuDFScalar::from_arrow_host(Scalar::new(&Int32Array::from(vec![42])))
            .expect("Failed to create scalar");
        assert!(scalar.as_any().downcast_ref::<CuDFScalar>().is_some());

        Ok(())
    }

    #[test]
    fn test_caching_works_int32() -> Result<(), Box<dyn std::error::Error>> {
        let scalar = CuDFScalar::from_arrow_host(Scalar::new(&Int32Array::from(vec![42])))
            .expect("Failed to create scalar");
        let size1 = scalar.get_buffer_memory_size();
        let size2 = scalar.get_buffer_memory_size();
        assert_eq!(size1, size2, "Cached and uncached sizes should match");

        Ok(())
    }

    #[test]
    fn test_caching_works_string() -> Result<(), Box<dyn std::error::Error>> {
        let scalar = CuDFScalar::from_arrow_host(Scalar::new(&StringArray::from(vec!["cached"])))
            .expect("Failed to create scalar");
        let size1 = scalar.get_buffer_memory_size();
        let size2 = scalar.get_buffer_memory_size();
        assert_eq!(size1, size2, "Cached and uncached sizes should match");

        Ok(())
    }

    #[test]
    fn test_caching_memory_sizes() -> Result<(), Box<dyn std::error::Error>> {
        let scalar = CuDFScalar::from_arrow_host(Scalar::new(&Int64Array::from(vec![12345])))
            .expect("Failed to create scalar");

        // Buffer size caching
        let size1 = scalar.get_buffer_memory_size();
        assert!(size1 > 0, "Buffer size should be positive");
        let size2 = scalar.get_buffer_memory_size();
        assert_eq!(size1, size2, "Cached buffer size should match");

        // Array size caching
        let array_size1 = scalar.get_array_memory_size();
        assert!(array_size1 > 0, "Array size should be positive");
        let array_size2 = scalar.get_array_memory_size();
        assert_eq!(array_size1, array_size2, "Cached array size should match");

        Ok(())
    }

    #[test]
    fn test_caching_clone_clears_cache() -> Result<(), Box<dyn std::error::Error>> {
        let scalar = CuDFScalar::from_arrow_host(Scalar::new(&Int32Array::from(vec![42])))
            .expect("Failed to create scalar");

        // Populate cache
        let _ = scalar.get_buffer_memory_size();

        // Clone should have empty cache
        let cloned = scalar.clone();

        assert!(scalar.get_buffer_memory_size() > 0);
        assert!(cloned.get_buffer_memory_size() > 0);

        Ok(())
    }

    #[test]
    fn test_caching_slice_clears_cache() -> Result<(), Box<dyn std::error::Error>> {
        let scalar = CuDFScalar::from_arrow_host(Scalar::new(&Int32Array::from(vec![42])))
            .expect("Failed to create scalar");

        // Populate original cache
        let original_size = scalar.get_buffer_memory_size();

        // Slice creates new instance with empty cache
        let sliced = scalar.slice(0, 1);
        let sliced_size = sliced.get_buffer_memory_size();

        // Both should have same size but independent caches
        assert_eq!(original_size, sliced_size);

        Ok(())
    }

    #[test]
    fn test_memory_size() -> Result<(), Box<dyn std::error::Error>> {
        let int32_scalar = CuDFScalar::from_arrow_host(Scalar::new(&Int32Array::from(vec![42])))
            .expect("Failed to create scalar");
        assert_memory_size(&int32_scalar, 4, "Int32");

        let int64_scalar = CuDFScalar::from_arrow_host(Scalar::new(&Int64Array::from(vec![12345])))
            .expect("Failed to create scalar");
        assert_memory_size(&int64_scalar, 8, "Int64");

        let float_scalar = CuDFScalar::from_arrow_host(Scalar::new(&Float64Array::from(vec![
            std::f64::consts::PI,
        ])))
        .expect("Failed to create scalar");
        assert_memory_size(&float_scalar, 8, "Float64");

        let bool_scalar = CuDFScalar::from_arrow_host(Scalar::new(&BooleanArray::from(vec![true])))
            .expect("Failed to create scalar");
        let buffer_size = bool_scalar.get_buffer_memory_size();
        let array_size = bool_scalar.get_array_memory_size();
        assert!(buffer_size > 0, "Boolean buffer size should be positive");
        assert!(
            array_size >= buffer_size,
            "Array size should be >= buffer size"
        );

        let test_string = "hello world";
        let string_scalar =
            CuDFScalar::from_arrow_host(Scalar::new(&StringArray::from(vec![test_string])))
                .expect("Failed to create scalar");
        let buffer_size = string_scalar.get_buffer_memory_size();
        let array_size = string_scalar.get_array_memory_size();
        // For strings, size should be at least the string length
        assert!(
            buffer_size >= test_string.len(),
            "Buffer size should be at least string length"
        );
        assert!(array_size >= buffer_size);

        let scalar = CuDFScalar::from_arrow_host(Scalar::new(&Int32Array::from(vec![None])))
            .expect("Failed to create scalar");
        let buffer_size = scalar.get_buffer_memory_size();
        let array_size = scalar.get_array_memory_size();
        // Null scalar should still have some memory overhead
        assert!(
            array_size >= buffer_size,
            "Array size should be >= buffer size"
        );

        Ok(())
    }

    #[test]
    fn test_null_handling() -> Result<(), Box<dyn std::error::Error>> {
        let null_scalar = CuDFScalar::from_arrow_host(Scalar::new(&Int32Array::from(vec![None])))
            .expect("Failed to create scalar");
        assert!(!null_scalar.inner().is_valid(), "Scalar should be null");

        let valid_scalar = CuDFScalar::from_arrow_host(Scalar::new(&Int32Array::from(vec![42])))
            .expect("Failed to create scalar");
        assert!(valid_scalar.inner().is_valid(), "Scalar should be valid");

        Ok(())
    }

    #[test]
    fn test_null_scalar_to_arrow() -> Result<(), Box<dyn std::error::Error>> {
        let scalar = CuDFScalar::from_arrow_host(Scalar::new(&Int32Array::from(vec![None])))?;
        let result = scalar.to_arrow_host()?;

        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), &DataType::Int32);
        assert_eq!(
            result.null_count(),
            1,
            "Null scalar should have null_count = 1"
        );

        Ok(())
    }

    #[test]
    fn test_data_conversions() -> Result<(), Box<dyn std::error::Error>> {
        let scalar = CuDFScalar::from_arrow_host(Scalar::new(&Int32Array::from(vec![42])))
            .expect("Failed to create scalar");

        let data = scalar.to_data();
        assert_eq!(data.len(), 1);
        assert_eq!(data.data_type(), &DataType::Int32);

        let scalar2 = CuDFScalar::from_arrow_host(Scalar::new(&Int32Array::from(vec![42])))
            .expect("Failed to create scalar");
        let data2 = scalar2.into_data();
        assert_eq!(data2.len(), 1);
        assert_eq!(data2.data_type(), &DataType::Int32);

        Ok(())
    }

    #[test]
    fn test_data_type_preserved_int32() -> Result<(), Box<dyn std::error::Error>> {
        let scalar = CuDFScalar::from_arrow_host(Scalar::new(&Int32Array::from(vec![42])))
            .expect("Failed to create scalar");
        assert_eq!(scalar.data_type(), &DataType::Int32);

        Ok(())
    }

    #[test]
    fn test_data_type_preserved_float64() -> Result<(), Box<dyn std::error::Error>> {
        let scalar = CuDFScalar::from_arrow_host(Scalar::new(&Float64Array::from(vec![
            std::f64::consts::PI,
        ])))
        .expect("Failed to create scalar");
        assert_eq!(scalar.data_type(), &DataType::Float64);

        Ok(())
    }

    #[test]
    fn test_data_type_preserved_string() -> Result<(), Box<dyn std::error::Error>> {
        let scalar = CuDFScalar::from_arrow_host(Scalar::new(&StringArray::from(vec!["test"])))
            .expect("Failed to create scalar");
        assert_eq!(scalar.data_type(), &DataType::Utf8);

        Ok(())
    }

    #[test]
    fn test_from_arrow_host_int32() -> Result<(), Box<dyn std::error::Error>> {
        let cudf_scalar = CuDFScalar::from_arrow_host(Scalar::new(&Int32Array::from(vec![42])))
            .expect("Failed to convert Int32");
        assert_eq!(cudf_scalar.data_type(), &DataType::Int32);

        Ok(())
    }

    #[test]
    fn test_from_arrow_host_int64() -> Result<(), Box<dyn std::error::Error>> {
        let cudf_scalar = CuDFScalar::from_arrow_host(Scalar::new(&Int64Array::from(vec![12345])))
            .expect("Failed to convert Int64");
        assert_eq!(cudf_scalar.data_type(), &DataType::Int64);

        Ok(())
    }

    #[test]
    fn test_from_arrow_host_float64() -> Result<(), Box<dyn std::error::Error>> {
        let cudf_scalar = CuDFScalar::from_arrow_host(Scalar::new(&Float64Array::from(vec![
            std::f64::consts::PI,
        ])))
        .expect("Failed to convert Float64");
        assert_eq!(cudf_scalar.data_type(), &DataType::Float64);

        Ok(())
    }

    #[test]
    fn test_from_arrow_host_string() -> Result<(), Box<dyn std::error::Error>> {
        let cudf_scalar =
            CuDFScalar::from_arrow_host(Scalar::new(&StringArray::from(vec!["hello"])))
                .expect("Failed to convert String");
        assert_eq!(cudf_scalar.data_type(), &DataType::Utf8);

        Ok(())
    }

    #[test]
    fn test_from_arrow_host_null() -> Result<(), Box<dyn std::error::Error>> {
        let cudf_scalar = CuDFScalar::from_arrow_host(Scalar::new(&Int32Array::from(vec![None])))
            .expect("Failed to convert null");
        assert_eq!(cudf_scalar.data_type(), &DataType::Int32);
        assert!(!cudf_scalar.inner().is_valid());

        Ok(())
    }

    #[test]
    fn test_scalar_clone() -> Result<(), Box<dyn std::error::Error>> {
        let original = CuDFScalar::from_arrow_host(Scalar::new(&Int32Array::from(vec![999])))
            .expect("Failed to create scalar");
        let cloned = original.clone();
        assert_eq!(cloned.data_type(), original.data_type());

        Ok(())
    }
}
