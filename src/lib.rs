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
//! ```ignore
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

use polars::prelude::*;

use arrow::compute;
use arrow::datatypes::{DataType, Field, FieldRef};
use arrow::record_batch::RecordBatch;
use arrow::array::{Array, StringArray, LargeStringArray, Date32Array};
use chrono::NaiveDate;
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
/// Converts string columns that contain date patterns to proper Arrow date types
fn convert_date_strings_to_dates(batch: RecordBatch) -> Result<RecordBatch> {
    let mut new_columns = Vec::new();
    let mut new_fields = Vec::new();
    
    for (i, field) in batch.schema().fields().iter().enumerate() {
        let column = batch.column(i);
        
        // Check if this is a string column that might contain dates
        if matches!(column.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
            // Try to extract string values from either StringArray or LargeStringArray
            let string_values: Vec<Option<&str>> = if let Some(string_array) = column.as_any().downcast_ref::<StringArray>() {
                string_array.iter().collect()
            } else if let Some(large_string_array) = column.as_any().downcast_ref::<LargeStringArray>() {
                large_string_array.iter().collect()
            } else {
                Vec::new()
            };
            
            if !string_values.is_empty() {
                // Check if all non-null values look like dates (YYYY-MM-DD format)
                let mut all_dates = true;
                let mut sample_count = 0;
                
                let mut date_format = None;
                
                for maybe_string in string_values.iter().take(10) { // Sample first 10 values
                    if let Some(string_val) = maybe_string {
                        sample_count += 1;
                        // Try to parse as different date/time formats
                        if NaiveDate::parse_from_str(string_val, "%Y-%m-%d").is_ok() {
                            if date_format.is_none() || date_format == Some("date") {
                                date_format = Some("date");
                            } else {
                                all_dates = false;
                                break;
                            }
                        // For now, only handle simple dates - datetime conversion has deserialization issues
                        } else {
                            all_dates = false;
                            break;
                        }
                    }
                }
                
                // If we found at least one value and they all parse as dates, convert
                if all_dates && sample_count > 0 && date_format.is_some() {
                    let format = date_format.unwrap();
                    
                    #[cfg(debug_assertions)]
                    eprintln!("Converting column '{}' from string to {}", field.name(), format);
                    
                    match format {
                        "date" => {
                            // Convert to Date32 array
                            let mut date_builder = Date32Array::builder(string_values.len());
                            
                            for maybe_string in string_values.iter() {
                                if let Some(string_val) = maybe_string {
                                    if let Ok(date) = NaiveDate::parse_from_str(string_val, "%Y-%m-%d") {
                                        // Convert to days since Unix epoch
                                        let days = date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32;
                                        date_builder.append_value(days);
                                    } else {
                                        date_builder.append_null();
                                    }
                                } else {
                                    date_builder.append_null();
                                }
                            }
                            
                            let date_array = date_builder.finish();
                            new_columns.push(Arc::new(date_array) as Arc<dyn Array>);
                            new_fields.push(Field::new(field.name(), DataType::Date32, field.is_nullable()));
                            continue;
                        },
                        // Datetime conversion disabled for now due to deserialization issues
                        _ => {
                            // Unknown format, keep as string
                        }
                    }
                }
            }
        }
        
        // Not a date column, keep as-is
        new_columns.push(column.clone());
        new_fields.push(field.as_ref().clone());
    }
    
    let new_schema = Arc::new(arrow::datatypes::Schema::new(new_fields));
    RecordBatch::try_new(new_schema, new_columns)
        .map_err(|e| PolarsSerdeError::ConversionError { 
            message: format!("Failed to create record batch with date conversion: {}", e) 
        })
}

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
/// ```ignore
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
/// ```ignore
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

    // Try from_type first for performance, fallback to from_samples for complex types like dates
    let fields: Vec<FieldRef> = Vec::<FieldRef>::from_type::<T>(tracing_options.clone())
        .or_else(|_| Vec::<FieldRef>::from_samples(rows, tracing_options))?;
    let rb: RecordBatch = to_record_batch(&fields, rows)?;

    // Convert any dictionary arrays to string arrays to avoid categorical requirements
    let mut converted_rb = convert_dictionary_to_strings(rb)?;
    
    // Convert date-like string columns to proper Arrow date types
    // Only convert simple date formats to avoid deserialization issues
    converted_rb = convert_date_strings_to_dates(converted_rb)?;

    let df: DataFrame = version_compat::arrow_to_dataframe(vec![converted_rb])?;
    Ok(df)
}

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

   
}
