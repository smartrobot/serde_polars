//! # Serde Polars
//!
//! High-performance serde integration for Polars DataFrames, providing seamless conversion
//! between Polars data structures and Rust structs that implement serde traits.
//!
//! This library automatically works with multiple Polars versions (0.40-0.50) through
//! feature flags, while maintaining a consistent API.
//!
//! ## Features
//!
//! - Convert Polars DataFrames to/from Vec<T> where T implements Serialize/Deserialize
//! - Multi-version compatibility for Polars 0.40+ via feature flags
//! - Thread-safe operations with comprehensive error handling
//! - High performance with minimal allocations
//! - Support for both single structs and collections
//!
//! ## Version Compatibility
//!
//! This crate supports Polars versions 0.40 through 0.50. You must:
//! 1. Include the Polars version you want to use in your `Cargo.toml`
//! 2. Enable the matching feature for serde_polars
//!
//! ```toml
//! [dependencies]
//! polars = "0.46"
//! serde_polars = { version = "0.1", default-features = false, features = ["polars-0-46"] }
//! ```
//!
//! **Important**: The Polars version and feature must match exactly!
//!
//! ## Quick Start
//!
//! ```rust
//! use polars::prelude::*;
//! use serde::{Serialize, Deserialize};
//! use serde_polars::{from_dataframe, to_dataframe};
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! struct Record {
//!     name: String,
//!     age: i32,
//!     score: f64,
//! }
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Create a DataFrame
//! let df = df!(
//!     "name" => ["Alice", "Bob", "Charlie"],
//!     "age" => [25, 30, 35],
//!     "score" => [85.5, 92.0, 78.3]
//! )?;
//!
//! // Convert DataFrame to structs
//! let records: Vec<Record> = from_dataframe(df)?;
//!
//! // Convert structs back to DataFrame
//! let new_df = to_dataframe(&records)?;
//! # Ok(())
//! # }
//! ```

#[cfg(feature = "polars")]
use polars::prelude::*;

use arrow::compute;
use arrow::datatypes::{DataType, Field, FieldRef};
use arrow::record_batch::RecordBatch;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_arrow::schema::{SchemaLike, TracingOptions};
use serde_arrow::{from_record_batch, to_record_batch};
use std::sync::Arc;

pub mod error;
pub mod version_compat;
pub use error::PolarsSerdeError;

/// Result type used throughout this crate
pub type Result<T> = std::result::Result<T, PolarsSerdeError>;

/// Helper function to convert dictionary arrays to string arrays to avoid categorical issues
fn convert_dictionary_to_strings(batch: RecordBatch) -> Result<RecordBatch> {
    let mut new_columns = Vec::new();
    let mut new_fields = Vec::new();
    let schema = batch.schema();

    for (i, column) in batch.columns().iter().enumerate() {
        let field = schema.field(i);

        match field.data_type() {
            DataType::Dictionary(_, value_type)
                if matches!(value_type.as_ref(), DataType::Utf8) =>
            {
                // Convert dictionary array to string array
                let string_array = compute::cast(column, &DataType::Utf8).map_err(|e| {
                    PolarsSerdeError::ConversionError {
                        message: format!("Failed to convert dictionary to string: {}", e),
                    }
                })?;
                new_columns.push(string_array);
                new_fields.push(Arc::new(Field::new(
                    field.name(),
                    DataType::Utf8,
                    field.is_nullable(),
                )));
            }
            _ => {
                new_columns.push(column.clone());
                new_fields.push(Arc::new(field.clone()));
            }
        }
    }

    let new_schema = Arc::new(arrow::datatypes::Schema::new(new_fields));
    RecordBatch::try_new(new_schema, new_columns).map_err(|e| PolarsSerdeError::ConversionError {
        message: format!("Failed to create converted record batch: {}", e),
    })
}

/// Convert a Polars DataFrame to Vec<T> where T implements Deserialize.
///
/// # Examples
///
/// ```rust
/// use polars::prelude::*;
/// use serde::Deserialize;
/// use serde_polars::from_dataframe;
///
/// #[derive(Debug, Deserialize)]
/// struct Record {
///     a: f64,
///     b: i64,
/// }
///
/// let df = df!(
///     "a" => [1.0, 2.0, 3.0],
///     "b" => [10i64, 20i64, 30i64]
/// )?;
///
/// let records: Vec<Record> = from_dataframe(df)?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn from_dataframe<T>(df: DataFrame) -> Result<Vec<T>>
where
    T: DeserializeOwned,
{
    let batches: Vec<RecordBatch> = version_compat::dataframe_to_arrow(df)?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let mut out = Vec::with_capacity(total_rows);

    for batch in &batches {
        let mut part: Vec<T> = from_record_batch(batch)?;
        out.append(&mut part);
    }
    Ok(out)
}

/// Convert Vec<T> to a Polars DataFrame where T implements Serialize and Deserialize.
///
/// # Examples
///
/// ```rust
/// use serde::{Serialize, Deserialize};
/// use serde_polars::to_dataframe;
///
/// #[derive(Debug, Serialize, Deserialize)]
/// struct Record {
///     a: f64,
///     b: i64,
/// }
///
/// let records = vec![
///     Record { a: 1.0, b: 10 },
///     Record { a: 2.0, b: 20 },
/// ];
///
/// let df = to_dataframe(&records)?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn to_dataframe<T>(rows: &Vec<T>) -> Result<DataFrame>
where
    T: Serialize + for<'de> serde::Deserialize<'de>,
{
    if rows.is_empty() {
        return Err(PolarsSerdeError::EmptyInput);
    }

    // Configure TracingOptions to support enums as strings and avoid dictionary arrays completely
    let tracing_options = TracingOptions::default()
        .enums_without_data_as_strings(true) // Convert simple enums to strings
        .allow_null_fields(true) // Allow nullable fields for better compatibility
        .map_as_struct(false) // Don't use struct for maps
        .string_dictionary_encoding(false) // Avoid dictionary encoding which requires categorical
        .coerce_numbers(false); // Be strict about types

    let fields: Vec<FieldRef> = Vec::<FieldRef>::from_type::<T>(tracing_options)?;
    let rb: RecordBatch = to_record_batch(&fields, rows)?;

    // Convert any dictionary arrays to string arrays to avoid categorical requirements
    let converted_rb = convert_dictionary_to_strings(rb)?;

    let df: DataFrame = version_compat::arrow_to_dataframe(vec![converted_rb])?;
    Ok(df)
}

// Legacy function names for backward compatibility
pub use from_dataframe as dataframe_to_structs;
pub use to_dataframe as structs_to_dataframe;

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestRecord {
        name: String,
        age: i32,
        score: f64,
        active: bool,
    }

    #[test]
    fn test_roundtrip_conversion() {
        let records = vec![
            TestRecord {
                name: "Alice".to_string(),
                age: 25,
                score: 85.5,
                active: true,
            },
            TestRecord {
                name: "Bob".to_string(),
                age: 30,
                score: 92.0,
                active: false,
            },
        ];

        let df = to_dataframe(&records).unwrap();
        let converted_back: Vec<TestRecord> = from_dataframe(df).unwrap();

        assert_eq!(records, converted_back);
    }

    #[test]
    fn test_empty_vec_error() {
        let records: Vec<TestRecord> = vec![];
        let result = to_dataframe(&records);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), PolarsSerdeError::EmptyInput));
    }

    #[test]
    fn test_legacy_function_names() {
        let records = vec![TestRecord {
            name: "Test".to_string(),
            age: 25,
            score: 85.5,
            active: true,
        }];

        // Test legacy names still work
        let df = structs_to_dataframe(&records).unwrap();
        let converted_back: Vec<TestRecord> = dataframe_to_structs(df).unwrap();

        assert_eq!(records, converted_back);
    }
}
