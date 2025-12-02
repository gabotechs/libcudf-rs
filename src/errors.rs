/// Error type for libcudf-rs operations
#[derive(Debug, thiserror::Error)]
pub enum CuDFError {
    /// Error from cuDF C++ library
    #[error("cuDF error: {0}")]
    CuDFError(#[from] cxx::Exception),

    /// Arrow error during conversion or other operations
    #[error(transparent)]
    ArrowError(#[from] arrow::error::ArrowError),
}

/// Result type alias for libcudf-rs operations
pub type Result<T> = std::result::Result<T, CuDFError>;
