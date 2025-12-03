#[cfg(test)]
mod tests {
    use arrow::array::{Array, Float64Array, Int32Array, Int64Array, Scalar, StringArray};
    use libcudf_rs::CuDFScalar;

    #[test]
    fn test_from_arrow_host_int32() {
        let array = Int32Array::from(vec![42]);
        let scalar = Scalar::new(&array);
        let cudf_scalar =
            CuDFScalar::from_arrow_host(scalar).expect("Failed to convert Arrow scalar to cuDF");

        assert_eq!(cudf_scalar.data_type(), &arrow_schema::DataType::Int32);
    }

    #[test]
    fn test_from_arrow_host_int64() {
        let array = Int64Array::from(vec![12345]);
        let scalar = Scalar::new(&array);
        let cudf_scalar =
            CuDFScalar::from_arrow_host(scalar).expect("Failed to convert Arrow scalar to cuDF");

        assert_eq!(cudf_scalar.data_type(), &arrow_schema::DataType::Int64);
    }

    #[test]
    fn test_from_arrow_host_float64() {
        let array = Float64Array::from(vec![std::f64::consts::PI]);
        let scalar = Scalar::new(&array);
        let cudf_scalar =
            CuDFScalar::from_arrow_host(scalar).expect("Failed to convert Arrow scalar to cuDF");

        assert_eq!(cudf_scalar.data_type(), &arrow_schema::DataType::Float64);
    }

    #[test]
    fn test_from_arrow_host_string() {
        let array = StringArray::from(vec!["hello"]);
        let scalar = Scalar::new(&array);
        let cudf_scalar = CuDFScalar::from_arrow_host(scalar)
            .expect("Failed to convert Arrow string scalar to cuDF");

        assert_eq!(cudf_scalar.data_type(), &arrow_schema::DataType::Utf8);
    }

    #[test]
    fn test_from_arrow_host_null() {
        let array = Int32Array::from(vec![None]);
        let scalar = Scalar::new(&array);
        let cudf_scalar = CuDFScalar::from_arrow_host(scalar)
            .expect("Failed to convert null Arrow scalar to cuDF");

        assert_eq!(cudf_scalar.data_type(), &arrow_schema::DataType::Int32);
        // The scalar should exist but represent a null value
        assert!(!cudf_scalar.inner().is_valid());
    }

    #[test]
    fn test_from_arrow_host_clone() {
        let array = Int32Array::from(vec![999]);
        let scalar = Scalar::new(&array);
        let cudf_scalar =
            CuDFScalar::from_arrow_host(scalar).expect("Failed to convert Arrow scalar to cuDF");

        // Test that we can clone the scalar
        let cloned = cudf_scalar.clone();
        assert_eq!(cloned.data_type(), cudf_scalar.data_type());
    }
}
