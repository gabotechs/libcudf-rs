use crate::errors::cudf_to_df;
use datafusion::common::exec_err;
use datafusion::error::DataFusionError;
use datafusion_expr::ColumnarValue;
use libcudf_rs::CuDFColumnView;
use std::sync::Arc;

mod binary_op;

pub(crate) fn columnar_value_to_cudf(c: ColumnarValue) -> Result<CuDFColumnView, DataFusionError> {
    match c {
        ColumnarValue::Array(arr) => CuDFColumnView::from_arrow(&arr).map_err(cudf_to_df),
        ColumnarValue::Scalar(_) => {
            exec_err!("ColumnarValue::Scalar is not allowed when executing in CuDF nodes")
        }
    }
}

pub(crate) fn cudf_to_columnar_value(view: CuDFColumnView) -> ColumnarValue {
    ColumnarValue::Array(Arc::new(view))
}
