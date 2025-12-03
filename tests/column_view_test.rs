use arrow::array::{Array, Int32Array};
use libcudf_rs::CuDFColumnView;

#[test]
fn test_column_view_operations() {
    // This test verifies that column views work correctly
    // The actual cloning is tested at the FFI level
    let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let column =
        CuDFColumnView::from_arrow(&array).expect("Failed to convert Arrow array to column");

    // Get pointer from column view
    let ptr = unsafe { column.data_ptr() };
    assert_ne!(ptr, 0, "Pointer should not be null");

    // Verify the column has the expected length
    assert_eq!(column.len(), 5);
    assert!(!column.is_empty());

    println!("✓ ColumnView operations verified for pointer {:#x}", ptr);
}

// Note: ColumnView cloning is primarily useful at the FFI level for internal operations.
// The clone() method allows cuDF operations to create multiple views of the same column data
// without copying the underlying GPU memory.
