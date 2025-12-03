use crate::{CuDFColumnView, CuDFScalar};
use arrow::array::Array;

pub enum CuDFColumnViewOrScalar {
    ColumnView(CuDFColumnView),
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

pub fn is_cudf_array(arr: &dyn Array) -> bool {
    let any = arr.as_any();
    any.is::<CuDFColumnView>() || any.is::<CuDFScalar>()
}
