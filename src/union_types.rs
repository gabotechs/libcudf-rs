use crate::{CuDFColumnView, CuDFScalar};

pub enum CudfColumnViewOrScalar {
    ColumnView(CuDFColumnView),
    Scalar(CuDFScalar),
}

impl From<CuDFColumnView> for CudfColumnViewOrScalar {
    fn from(col: CuDFColumnView) -> Self {
        Self::ColumnView(col)
    }
}

impl From<CuDFScalar> for CudfColumnViewOrScalar {
    fn from(scalar: CuDFScalar) -> Self {
        Self::Scalar(scalar)
    }
}
