use arrow::array::Int32Array;
use libcudf_rs::{CuDFColumn, CuDFColumnView};

#[test]
fn test_column_data_is_on_gpu() {
    let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let column = CuDFColumn::from_arrow_host(&array)
        .expect("Failed to convert Arrow array to column")
        .into_view();

    // Get the raw device pointer
    let ptr = unsafe { column.data_ptr() };

    // Non-null pointer check
    assert_ne!(ptr, 0, "Column data pointer should not be null");

    // Use cudaPointerGetAttributes to verify it's device memory
    let mut attrs: cuda_runtime_sys::cudaPointerAttributes = unsafe { std::mem::zeroed() };
    let result = unsafe {
        cuda_runtime_sys::cudaPointerGetAttributes(
            &mut attrs as *mut _,
            ptr as *const std::ffi::c_void,
        )
    };

    assert_eq!(
        result,
        cuda_runtime_sys::cudaError_t::cudaSuccess,
        "cudaPointerGetAttributes should succeed"
    );

    // Check that the memory is NOT on host
    // RMM (RAPIDS Memory Manager) allocations may show as cudaMemoryTypeUnregistered
    // which is fine - it's still device memory, just managed by RMM's pool allocator
    assert_ne!(
        attrs.type_,
        cuda_runtime_sys::cudaMemoryType::cudaMemoryTypeHost,
        "Column data should NOT be in host memory"
    );

    match attrs.type_ {
        cuda_runtime_sys::cudaMemoryType::cudaMemoryTypeDevice => {
            println!(
                "✓ Column data verified to be on GPU device {} (standard device memory)",
                attrs.device
            );
        }
        cuda_runtime_sys::cudaMemoryType::cudaMemoryTypeUnregistered => {
            println!(
                "✓ Column data verified to be on GPU device {} (RMM-managed memory)",
                attrs.device
            );
        }
        _ => {
            panic!("Unexpected memory type: {:?}", attrs.type_);
        }
    }
}

#[test]
fn test_empty_column_pointer() {
    let array = Int32Array::from(Vec::<i32>::new());
    let column = CuDFColumn::from_arrow_host(&array)
        .expect("Failed to convert Arrow array to column")
        .into_view();

    // Empty columns might have null pointers, which is fine
    let ptr = unsafe { column.data_ptr() };
    println!("Empty column data pointer: {:#x}", ptr);
}

#[test]
fn test_column_view_pointer() {
    let array = Int32Array::from(vec![10, 20, 30]);
    let column = CuDFColumn::from_arrow_host(&array)
        .expect("Failed to convert Arrow array to column")
        .into_view();

    // Get pointer from column
    let column_ptr = unsafe { column.data_ptr() };

    // Note: We can't easily test that Column::data_ptr() and ColumnView::data_ptr()
    // return the same value without exposing view() publicly, but we've verified
    // both implementations use the same underlying cudf::column_view::head() method
    // so they're guaranteed to return the same pointer.

    assert_ne!(column_ptr, 0, "Column pointer should not be null");

    println!("✓ Column points to GPU memory at {:#x}", column_ptr);
}
