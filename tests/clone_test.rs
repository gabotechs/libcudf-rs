#[cfg(test)]
mod tests {
    use arrow::array::{Array, Int32Array};
    use libcudf_rs::CuDFColumnView;

    #[test]
    fn test_column_view_clone() {
        let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let column =
            CuDFColumnView::from_arrow(&array).expect("Failed to convert Arrow array to column");

        // Clone the column view
        let cloned = column.clone();

        // Both should have the same length
        assert_eq!(column.len(), 5);
        assert_eq!(cloned.len(), 5);

        // Both should point to the same GPU memory
        let original_ptr = unsafe { column.data_ptr() };
        let cloned_ptr = unsafe { cloned.data_ptr() };
        assert_eq!(
            original_ptr, cloned_ptr,
            "Cloned view should point to the same GPU memory"
        );

        println!(
            "✓ Cloned ColumnView verified: both point to GPU memory at {:#x}",
            original_ptr
        );
    }

    #[test]
    fn test_multiple_clones() {
        let array = Int32Array::from(vec![10, 20, 30]);
        let column =
            CuDFColumnView::from_arrow(&array).expect("Failed to convert Arrow array to column");

        // Create multiple clones
        let clone1 = column.clone();
        let clone2 = column.clone();
        let clone3 = clone1.clone();

        // All should point to the same GPU memory
        let ptr = unsafe { column.data_ptr() };
        assert_eq!(unsafe { clone1.data_ptr() }, ptr);
        assert_eq!(unsafe { clone2.data_ptr() }, ptr);
        assert_eq!(unsafe { clone3.data_ptr() }, ptr);

        // All should have the same length
        assert_eq!(column.len(), 3);
        assert_eq!(clone1.len(), 3);
        assert_eq!(clone2.len(), 3);
        assert_eq!(clone3.len(), 3);

        // Drop original, clones should still be valid
        drop(column);
        assert_eq!(unsafe { clone1.data_ptr() }, ptr);
        assert_eq!(clone1.len(), 3);

        println!("✓ Multiple clones work correctly with Arc-based lifetime management");
    }
}
