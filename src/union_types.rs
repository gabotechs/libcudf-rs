use crate::{CuDFColumnView, CuDFScalar};

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
