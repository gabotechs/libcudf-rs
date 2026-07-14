/// Error type for libcudf-rs operations
#[derive(Debug, thiserror::Error)]
pub enum CuDFError {
    /// Error from cuDF C++ library
    #[error("cuDF error: {0}")]
    CuDFError(#[from] cxx::Exception),

    /// Arrow error during conversion or other operations
    #[error(transparent)]
    ArrowError(#[from] arrow::error::ArrowError),

    /// cuDF returned a null handle where a valid handle was required
    #[error("cuDF returned a null {0} handle")]
    NullHandle(&'static str),

    /// Invalid or conflicting high-level resource configuration
    #[error("cuDF configuration error: {0}")]
    Configuration(String),

    /// CUDA Runtime API failure.
    #[error("CUDA error {code}: {message}")]
    Cuda { code: i32, message: String },
}

/// Result type alias for libcudf-rs operations
pub type Result<T> = std::result::Result<T, CuDFError>;

pub(crate) fn cudf_size_to_usize(value: i32, name: &'static str) -> Result<usize> {
    usize::try_from(value).map_err(|_| {
        arrow::error::ArrowError::ComputeError(format!("cuDF returned a negative {name}: {value}"))
            .into()
    })
}

pub(crate) fn usize_to_cudf_size(value: usize, name: &'static str) -> Result<i32> {
    i32::try_from(value).map_err(|_| {
        arrow::error::ArrowError::InvalidArgumentError(format!(
            "{name} {value} exceeds cudf::size_type"
        ))
        .into()
    })
}
