//! Error types for polars_serde operations

use thiserror::Error;

#[cfg(feature = "polars_0_40")]
use polars_crate_0_40 as polars;

#[cfg(feature = "polars_0_41")]
use polars_crate_0_41 as polars;

#[cfg(feature = "polars_0_42")]
use polars_crate_0_42 as polars;

#[cfg(feature = "polars_0_43")]
use polars_crate_0_43 as polars;

#[cfg(feature = "polars_0_44")]
use polars_crate_0_44 as polars;

#[cfg(feature = "polars_0_45")]
use polars_crate_0_45 as polars;

#[cfg(feature = "polars_0_46")]
use polars_crate_0_46 as polars;

#[cfg(feature = "polars_0_47")]
use polars_crate_0_47 as polars;

#[cfg(feature = "polars_0_48")]
use polars_crate_0_48 as polars;

#[cfg(feature = "polars_0_49")]
use polars_crate_0_49 as polars;

#[cfg(feature = "polars_0_50")]
use polars_crate_0_50 as polars;

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
