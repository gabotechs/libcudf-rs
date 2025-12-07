use crate::{CuDFColumnView, CuDFScalar};
use arrow::array::Array;

/// An enum that can hold either a cuDF column view or a scalar
///
/// This type is used in operations that can accept either columns or scalar values,
/// such as binary operations where you might want to add a constant to every element
/// of a column.
pub enum CuDFColumnViewOrScalar {
    /// A column view containing multiple values
    ColumnView(CuDFColumnView),
    /// A single scalar value
    Scalar(CuDFScalar),
}

impl From<CuDFColumnView> for CuDFColumnViewOrScalar {
    fn from(col: CuDFColumnView) -> Self {
        Self::ColumnView(col)
    }
}

impl From<CuDFScalar> for CuDFColumnViewOrScalar {
    fn from(scalar: CuDFScalar) -> Self {
        Self::Scalar(scalar)
    }
}

/// Check if an Arrow array is actually a cuDF array (stored in GPU memory)
///
/// Returns `true` if the array is a `CuDFColumnView` or `CuDFScalar`,
/// `false` for regular Arrow arrays stored in host memory.
///
/// # Examples
///
/// ```no_run
/// use arrow::array::{Int32Array, Array};
/// use libcudf_rs::{CuDFColumn, is_cudf_array};
///
/// let host_array = Int32Array::from(vec![1, 2, 3]);
/// assert!(!is_cudf_array(&host_array));
///
/// let gpu_array = CuDFColumn::from_arrow_host(&host_array)?.into_view();
/// assert!(is_cudf_array(&gpu_array));
/// # Ok::<(), libcudf_rs::CuDFError>(())
/// ```
pub fn is_cudf_array(arr: &dyn Array) -> bool {
    let any = arr.as_any();
    any.is::<CuDFColumnView>() || any.is::<CuDFScalar>()
}

/// Check if a RecordBatch contains only cuDF arrays (stored in GPU memory)
///
/// Returns `true` if all columns in the RecordBatch are cuDF arrays (`CuDFColumnView` or `CuDFScalar`),
/// `false` if any column is a regular Arrow array stored in host memory.
///
/// This is useful to verify that a RecordBatch is fully GPU-resident before passing it to
/// GPU-accelerated operations or when implementing physical plan operators that require GPU data.
///
/// # Examples
///
/// ```no_run
/// # use arrow::array::{Int32Array, RecordBatch};
/// # use arrow::datatypes::{DataType, Field, Schema};
/// # use libcudf_rs::{CuDFColumn, is_cudf_record_batch};
/// # use std::sync::Arc;
///
/// // Create a host RecordBatch
/// let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
/// let array = Int32Array::from(vec![1, 2, 3]);
/// let host_batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(array)])?;
/// assert!(!is_cudf_record_batch(&host_batch));
///
/// // Create a GPU RecordBatch
/// let gpu_array = CuDFColumn::from_arrow_host(&Int32Array::from(vec![1, 2, 3]))?.into_view();
/// let gpu_batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(gpu_array)])?;
/// assert!(is_cudf_record_batch(&gpu_batch));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn is_cudf_record_batch(batch: &arrow::record_batch::RecordBatch) -> bool {
    batch.columns().iter().all(|col| is_cudf_array(col))
}
