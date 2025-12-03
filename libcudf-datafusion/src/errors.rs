use datafusion::error::DataFusionError;
use libcudf_rs::CuDFError;

pub(crate) fn cudf_to_df(err: CuDFError) -> DataFusionError {
    match err {
        CuDFError::CuDFError(err) => DataFusionError::External(Box::new(err)),
        CuDFError::ArrowError(err) => DataFusionError::ArrowError(Box::new(err), None),
    }
}
