use arrow::util::pretty::pretty_format_batches;
use std::sync::Arc;

/// Format a table as a string for debugging/testing purposes
///
/// This converts the table to Arrow format and uses Arrow's pretty printing.
///
/// # Returns
///
/// A formatted string representation of the table, or an error if formatting fails.
///
/// # Examples
///
/// ```ignore
/// let table = ffi::read_parquet("data.parquet")?;
/// let formatted = print_table(&table)?;
/// println!("{}", formatted);
/// ```
pub fn print_table(table: &libcudf_sys::ffi::Table) -> Result<String, std::fmt::Error> {
    let mut ffi_schema = arrow::ffi::FFI_ArrowSchema::empty();
    let mut device_array = libcudf_sys::ArrowDeviceArray::empty();

    libcudf_sys::table_to_arrow_schema(table, &mut ffi_schema)
        .map_err(|_| std::fmt::Error)?;
    libcudf_sys::table_to_arrow_array(table, &mut device_array)
        .map_err(|_| std::fmt::Error)?;

    let schema = Arc::new(
        arrow::datatypes::Schema::try_from(&ffi_schema)
            .map_err(|_| std::fmt::Error)?
    );

    let array_data = unsafe { arrow::ffi::from_ffi(device_array.array, &ffi_schema) }
        .map_err(|_| std::fmt::Error)?;

    let struct_array = arrow::array::StructArray::from(array_data);

    let batch = arrow::record_batch::RecordBatch::try_new(schema, struct_array.columns().to_vec())
        .map_err(|_| std::fmt::Error)?;

    let formatted = pretty_format_batches(&[batch])
        .map_err(|_| std::fmt::Error)?;

    Ok(formatted.to_string())
}
