//! Error types for polars_serde operations

use thiserror::Error;

/// Comprehensive error type for all polars_serde operations
#[derive(Error, Debug)]
pub enum PolarsSerdeError {
    /// Polars-specific errors
    #[error("Polars error: {0}")]
    PolarsError(#[from] polars::error::PolarsError),

    /// Serde Arrow conversion errors
    #[error("Serde Arrow error: {0}")]
    SerdeArrowError(#[from] serde_arrow::Error),

    /// Arrow-related errors
    #[error("Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),

    /// DataFrame interchange errors
    #[error("Interchange error: {0}")]
    InterchangeError(#[from] df_interchange::InterchangeError),

    /// Invalid row count errors
    #[error("Invalid row count: expected {expected}, got {actual}")]
    InvalidRowCount { expected: usize, actual: usize },

    /// Empty input errors
    #[error("Cannot create DataFrame from empty input")]
    EmptyInput,

    /// Generic conversion errors
    #[error("Conversion error: {message}")]
    ConversionError { message: String },
}
