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
/// use libcudf_rs::{CuDFColumnView, is_cudf_array};
///
/// let host_array = Int32Array::from(vec![1, 2, 3]);
/// assert!(!is_cudf_array(&host_array));
///
/// let gpu_array = CuDFColumnView::from_arrow(&host_array)?;
/// assert!(is_cudf_array(&gpu_array));
/// # Ok::<(), libcudf_rs::CuDFError>(())
/// ```
pub fn is_cudf_array(arr: &dyn Array) -> bool {
    let any = arr.as_any();
    any.is::<CuDFColumnView>() || any.is::<CuDFScalar>()
}
