#![allow(dead_code)]

use arrow::array::{make_array, RecordBatch, StructArray};
use arrow::ffi::{from_ffi, from_ffi_and_data_type, FFI_ArrowArray};
use arrow::util::pretty::{pretty_format_batches, pretty_format_columns};
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::{ArrowError, DataType};
use libcudf_sys::ffi::{ColumnView, TableView};
use std::fmt::Display;

pub fn pretty_table(table_view: &TableView) -> Result<impl Display + use<>, ArrowError> {
    let mut array = FFI_ArrowArray::empty();
    let mut schema = FFI_ArrowSchema::empty();

    let data = unsafe {
        table_view.to_arrow_array(&mut array as *mut FFI_ArrowArray as *mut u8);
        table_view.to_arrow_schema(&mut schema as *mut FFI_ArrowSchema as *mut u8);

        from_ffi(array, &schema).expect("ffi data should be valid")
    };

    let record = RecordBatch::from(StructArray::from(data));

    pretty_format_batches(&[record])
}

pub fn pretty_column(
    column_view: &ColumnView,
    data_type: DataType,
) -> Result<impl Display + use<>, ArrowError> {
    let mut array = FFI_ArrowArray::empty();

    let data = unsafe {
        column_view.to_arrow_array(&mut array as *mut FFI_ArrowArray as *mut u8);

        from_ffi_and_data_type(array, data_type).expect("ffi data should be valid")
    };

    let array = make_array(data);
    pretty_format_columns("test", &[array])
}
