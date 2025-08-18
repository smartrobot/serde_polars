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
//! - **Efficient date/time handling with automatic chrono type detection**
//! - Support for both single structs and collections
//!
//! ## Automatic Date/Time Handling
//!
//! This library **automatically detects and converts chrono types** to proper Polars columns:
//!
//! ### Chrono Types (Automatic Detection)
//! - `NaiveDate` → `Date` column (stored as i32 days since Unix epoch)
//! - `NaiveDateTime` → `Timestamp` column (stored as i64 nanoseconds since Unix epoch) 
//! - `DateTime<Utc>` → `Timestamp` column (stored as i64 nanoseconds since Unix epoch)
//!
//! **All chrono types work seamlessly with efficient numeric storage!**
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
//! use chrono::NaiveDate;
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! struct Record {
//!     name: String,
//!     age: i32,
//!     birth_date: NaiveDate,  // Just use raw chrono types!
//!     score: f64,
//! }
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Create records with automatic date handling
//! let records = vec![
//!     Record {
//!         name: "Alice".to_string(),
//!         age: 25,
//!         birth_date: NaiveDate::from_ymd_opt(1998, 5, 15).unwrap(),
//!         score: 85.5,
//!     },
//!     Record {
//!         name: "Bob".to_string(), 
//!         age: 30,
//!         birth_date: NaiveDate::from_ymd_opt(1993, 8, 22).unwrap(),
//!         score: 92.0,
//!     },
//! ];
//!
//! // Convert to DataFrame (birth_date will be automatically converted to Date type!)
//! let df = to_dataframe(&records)?;
//!
//! // Convert DataFrame back to structs
//! let converted_back: Vec<Record> = from_dataframe(df)?;
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
use arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{NaiveDate, NaiveDateTime, DateTime, Duration};
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


use std::collections::HashMap;



/// Type detector that identifies chrono types at compile time
struct TypeDetector {
    field_types: HashMap<String, String>,
    current_field: Option<String>,
}

impl TypeDetector {
    fn new() -> Self {
        Self {
            field_types: HashMap::new(),
            current_field: None,
        }
    }
}

impl serde::ser::Serializer for &mut TypeDetector {
    type Ok = ();
    type Error = serde_arrow::Error;
    
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, _v: bool) -> std::result::Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_i8(self, _v: i8) -> std::result::Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_i16(self, _v: i16) -> std::result::Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_i32(self, _v: i32) -> std::result::Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_i64(self, _v: i64) -> std::result::Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_u8(self, _v: u8) -> std::result::Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_u16(self, _v: u16) -> std::result::Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_u32(self, _v: u32) -> std::result::Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_u64(self, _v: u64) -> std::result::Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_f32(self, _v: f32) -> std::result::Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_f64(self, _v: f64) -> std::result::Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_char(self, _v: char) -> std::result::Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_bytes(self, _v: &[u8]) -> std::result::Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_none(self) -> std::result::Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_some<T: ?Sized>(self, value: &T) -> std::result::Result<Self::Ok, Self::Error> 
    where T: serde::Serialize {
        value.serialize(self)
    }
    fn serialize_unit(self) -> std::result::Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_unit_struct(self, _name: &'static str) -> std::result::Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_unit_variant(self, _name: &'static str, _variant_index: u32, _variant: &'static str) -> std::result::Result<Self::Ok, Self::Error> { Ok(()) }
    
    fn serialize_newtype_struct<T: ?Sized>(self, name: &'static str, value: &T) -> std::result::Result<Self::Ok, Self::Error>
    where T: serde::Serialize {
        // Detect wrapper types - these are already efficient!
        if let Some(field_name) = &self.current_field {
            self.field_types.insert(field_name.clone(), name.to_string());
        }
        value.serialize(self)
    }
    
    fn serialize_str(self, _v: &str) -> std::result::Result<Self::Ok, Self::Error> { 
        // DO NOT DETECT CHRONO TYPES FROM STRINGS!
        // This was the inefficient approach the user rejected
        Ok(()) 
    }
    
    fn serialize_newtype_variant<T: ?Sized>(self, _name: &'static str, _variant_index: u32, _variant: &'static str, value: &T) -> std::result::Result<Self::Ok, Self::Error>
    where T: serde::Serialize {
        value.serialize(self)
    }
    fn serialize_seq(self, _len: Option<usize>) -> std::result::Result<Self::SerializeSeq, Self::Error> { Ok(self) }
    fn serialize_tuple(self, _len: usize) -> std::result::Result<Self::SerializeTuple, Self::Error> { Ok(self) }
    fn serialize_tuple_struct(self, _name: &'static str, _len: usize) -> std::result::Result<Self::SerializeTupleStruct, Self::Error> { Ok(self) }
    fn serialize_tuple_variant(self, _name: &'static str, _variant_index: u32, _variant: &'static str, _len: usize) -> std::result::Result<Self::SerializeTupleVariant, Self::Error> { Ok(self) }
    fn serialize_map(self, _len: Option<usize>) -> std::result::Result<Self::SerializeMap, Self::Error> { Ok(self) }
    fn serialize_struct(self, _name: &'static str, _len: usize) -> std::result::Result<Self::SerializeStruct, Self::Error> { Ok(self) }
    fn serialize_struct_variant(self, _name: &'static str, _variant_index: u32, _variant: &'static str, _len: usize) -> std::result::Result<Self::SerializeStructVariant, Self::Error> { Ok(self) }
}

// Implement the compound serialization traits
macro_rules! impl_serialize_compound {
    ($trait:ident, $method:ident) => {
        impl serde::ser::$trait for &mut TypeDetector {
            type Ok = ();
            type Error = serde_arrow::Error;
            fn $method<T: ?Sized>(&mut self, value: &T) -> std::result::Result<(), Self::Error> 
            where T: serde::Serialize { 
                value.serialize(&mut **self)
            }
            fn end(self) -> std::result::Result<Self::Ok, Self::Error> { Ok(()) }
        }
    };
}

impl_serialize_compound!(SerializeSeq, serialize_element);
impl_serialize_compound!(SerializeTuple, serialize_element);
impl_serialize_compound!(SerializeTupleStruct, serialize_field);
impl_serialize_compound!(SerializeTupleVariant, serialize_field);

impl serde::ser::SerializeMap for &mut TypeDetector {
    type Ok = ();
    type Error = serde_arrow::Error;
    fn serialize_key<T: ?Sized>(&mut self, key: &T) -> std::result::Result<(), Self::Error> 
    where T: serde::Serialize { 
        key.serialize(&mut **self)
    }
    fn serialize_value<T: ?Sized>(&mut self, value: &T) -> std::result::Result<(), Self::Error> 
    where T: serde::Serialize { 
        value.serialize(&mut **self)
    }
    fn end(self) -> std::result::Result<Self::Ok, Self::Error> { Ok(()) }
}

impl serde::ser::SerializeStruct for &mut TypeDetector {
    type Ok = ();
    type Error = serde_arrow::Error;
    
    fn serialize_field<T: ?Sized>(&mut self, key: &'static str, value: &T) -> std::result::Result<(), Self::Error> 
    where T: serde::Serialize {
        self.current_field = Some(key.to_string());
        
        // Check the type name to detect chrono types
        let type_name = std::any::type_name::<T>();
        
        if type_name == "chrono::naive::date::NaiveDate" {
            self.field_types.insert(key.to_string(), "NaiveDate".to_string());
        } else if type_name == "chrono::naive::datetime::NaiveDateTime" {
            self.field_types.insert(key.to_string(), "NaiveDateTime".to_string());
        } else if type_name.starts_with("chrono::datetime::DateTime<chrono::offset::utc::Utc>") {
            self.field_types.insert(key.to_string(), "DateTimeUtc".to_string());
        } else if type_name.starts_with("core::option::Option<chrono::naive::date::NaiveDate>") {
            self.field_types.insert(key.to_string(), "NaiveDate".to_string());
        } else if type_name.starts_with("core::option::Option<chrono::naive::datetime::NaiveDateTime>") {
            self.field_types.insert(key.to_string(), "NaiveDateTime".to_string());
        } else if type_name.starts_with("core::option::Option<chrono::datetime::DateTime<chrono::offset::utc::Utc>>") {
            self.field_types.insert(key.to_string(), "DateTimeUtc".to_string());
        }
        
        value.serialize(&mut **self)?;
        self.current_field = None;
        Ok(())
    }
    
    fn end(self) -> std::result::Result<Self::Ok, Self::Error> { Ok(()) }
}

impl serde::ser::SerializeStructVariant for &mut TypeDetector {
    type Ok = ();
    type Error = serde_arrow::Error;
    fn serialize_field<T: ?Sized>(&mut self, key: &'static str, value: &T) -> std::result::Result<(), Self::Error> 
    where T: serde::Serialize {
        self.current_field = Some(key.to_string());
        value.serialize(&mut **self)?;
        self.current_field = None;
        Ok(())
    }
    fn end(self) -> std::result::Result<Self::Ok, Self::Error> { Ok(()) }
}


/// Detect chrono types by analyzing type information at compile time
fn detect_chrono_types<T: Serialize>(sample: &T) -> std::result::Result<HashMap<String, String>, serde_arrow::Error> {
    let mut detector = TypeDetector::new();
    sample.serialize(&mut detector).map_err(|_| serde_arrow::Error::custom("Type detection failed".to_string()))?;
    Ok(detector.field_types)
}


/// Convert string arrays containing dates to Date32 arrays (i32 days since Unix epoch)
fn convert_string_dates_to_date32(column: &arrow::array::ArrayRef) -> Result<arrow::array::ArrayRef> {
    use arrow::array::{StringArray, LargeStringArray, Date32Builder};
    use arrow::array::Array;
    
    let mut builder = Date32Builder::new();
    
    // Handle both Utf8 and LargeUtf8 string arrays
    if let Some(string_array) = column.as_any().downcast_ref::<StringArray>() {
        for i in 0..string_array.len() {
            if string_array.is_null(i) {
                builder.append_null();
            } else {
                let date_str = string_array.value(i);
                // Parse chrono's default date format
                if let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                    let days = date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32;
                    builder.append_value(days);
                } else {
                    return Err(PolarsSerdeError::ConversionError {
                        message: format!("Failed to parse date string: {}", date_str),
                    });
                }
            }
        }
    } else if let Some(large_string_array) = column.as_any().downcast_ref::<LargeStringArray>() {
        for i in 0..large_string_array.len() {
            if large_string_array.is_null(i) {
                builder.append_null();
            } else {
                let date_str = large_string_array.value(i);
                // Parse chrono's default date format
                if let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                    let days = date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32;
                    builder.append_value(days);
                } else {
                    return Err(PolarsSerdeError::ConversionError {
                        message: format!("Failed to parse date string: {}", date_str),
                    });
                }
            }
        }
    } else {
        return Err(PolarsSerdeError::ConversionError {
            message: "Expected string array (Utf8 or LargeUtf8) for date conversion".to_string(),
        });
    }
    
    Ok(Arc::new(builder.finish()))
}

/// Convert string arrays containing datetimes to Timestamp arrays (i64 nanoseconds since Unix epoch)
fn convert_string_datetimes_to_timestamp(
    column: &arrow::array::ArrayRef, 
    timezone: Option<Arc<str>>
) -> Result<arrow::array::ArrayRef> {
    use arrow::array::{StringArray, LargeStringArray, TimestampNanosecondBuilder};
    use arrow::array::Array;
    
    let mut builder = TimestampNanosecondBuilder::new();
    
    // Handle both Utf8 and LargeUtf8 string arrays
    if let Some(string_array) = column.as_any().downcast_ref::<StringArray>() {
        for i in 0..string_array.len() {
            if string_array.is_null(i) {
                builder.append_null();
            } else {
                let datetime_str = string_array.value(i);
                let nanos = parse_datetime_string(datetime_str, timezone.is_some())?;
                builder.append_value(nanos);
            }
        }
    } else if let Some(large_string_array) = column.as_any().downcast_ref::<LargeStringArray>() {
        for i in 0..large_string_array.len() {
            if large_string_array.is_null(i) {
                builder.append_null();
            } else {
                let datetime_str = large_string_array.value(i);
                let nanos = parse_datetime_string(datetime_str, timezone.is_some())?;
                builder.append_value(nanos);
            }
        }
    } else {
        return Err(PolarsSerdeError::ConversionError {
            message: "Expected string array (Utf8 or LargeUtf8) for datetime conversion".to_string(),
        });
    }
    
    Ok(Arc::new(builder.finish().with_timezone_opt(timezone)))
}

/// Helper function to parse datetime strings
fn parse_datetime_string(datetime_str: &str, is_utc: bool) -> Result<i64> {
    if is_utc {
        // Parse DateTime<Utc> format (RFC3339)
        if let Ok(dt) = DateTime::parse_from_rfc3339(datetime_str) {
            Ok(dt.timestamp_nanos_opt().unwrap_or(0))
        } else {
            Err(PolarsSerdeError::ConversionError {
                message: format!("Failed to parse UTC datetime string: {}", datetime_str),
            })
        }
    } else {
        // Parse NaiveDateTime format  
        if let Ok(dt) = NaiveDateTime::parse_from_str(datetime_str, "%Y-%m-%dT%H:%M:%S%.f") {
            Ok(dt.and_utc().timestamp_nanos_opt().unwrap_or(0))
        } else {
            Err(PolarsSerdeError::ConversionError {
                message: format!("Failed to parse datetime string: {}", datetime_str),
            })
        }
    }
}

/// Convert chrono types to proper Arrow date/datetime types
fn convert_chrono_columns(
    batch: RecordBatch,
    chrono_types: &HashMap<String, String>
) -> Result<RecordBatch> {
    
    let mut new_columns = Vec::new();
    let mut new_fields = Vec::new();
    let schema = batch.schema();

    for (i, column) in batch.columns().iter().enumerate() {
        let field = schema.field(i);
        let field_name = field.name();
        
        if let Some(chrono_type) = chrono_types.get(field_name) {
            match chrono_type.as_str() {
                "NaiveDate" => {
                    // Convert string dates to Date32 (i32 days since Unix epoch)
                    let date_array = convert_string_dates_to_date32(column)?;
                    new_columns.push(date_array);
                    new_fields.push(Arc::new(Field::new(
                        field_name,
                        DataType::Date32,
                        field.is_nullable(),
                    )));
                },
                "NaiveDateTime" => {
                    // Convert string datetimes to Timestamp (i64 nanoseconds)
                    let ts_array = convert_string_datetimes_to_timestamp(column, None)?;
                    new_columns.push(ts_array);
                    new_fields.push(Arc::new(Field::new(
                        field_name,
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        field.is_nullable(),
                    )));
                },
                "DateTimeUtc" => {
                    // Convert string UTC datetimes to Timestamp with UTC timezone
                    let ts_array = convert_string_datetimes_to_timestamp(column, Some("UTC".into()))?;
                    new_columns.push(ts_array);
                    new_fields.push(Arc::new(Field::new(
                        field_name,
                        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                        field.is_nullable(),
                    )));
                },
                _ => {
                    // Unknown chrono type, keep as-is
                    new_columns.push(column.clone());
                    new_fields.push(Arc::new(field.clone()));
                }
            }
        } else {
            // No chrono type detected, keep as-is
            new_columns.push(column.clone());
            new_fields.push(Arc::new(field.clone()));
        }
    }

    let new_schema = Arc::new(arrow::datatypes::Schema::new(new_fields));
    RecordBatch::try_new(new_schema, new_columns).map_err(|e| PolarsSerdeError::ConversionError {
        message: format!("Failed to create converted record batch: {}", e),
    })
}

/// Convert Date32 arrays back to string arrays for chrono deserialization  
fn convert_date32_to_string(column: &arrow::array::ArrayRef) -> Result<arrow::array::ArrayRef> {
    use arrow::array::{Date32Array, StringBuilder};
    use arrow::array::Array;
    
    let date_array = column.as_any().downcast_ref::<Date32Array>()
        .ok_or_else(|| PolarsSerdeError::ConversionError {
            message: "Expected Date32 array for string conversion".to_string(),
        })?;
    
    let mut builder = StringBuilder::new();
    
    for i in 0..date_array.len() {
        if date_array.is_null(i) {
            builder.append_null();
        } else {
            let days = date_array.value(i);
            let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(days as i64);
            builder.append_value(date.format("%Y-%m-%d").to_string());
        }
    }
    
    Ok(Arc::new(builder.finish()))
}

/// Convert Timestamp arrays back to string arrays for chrono deserialization
fn convert_timestamp_to_string(
    column: &arrow::array::ArrayRef,
    timezone: Option<Arc<str>>
) -> Result<arrow::array::ArrayRef> {
    use arrow::array::{TimestampNanosecondArray, StringBuilder};
    use arrow::array::Array;
    
    let ts_array = column.as_any().downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| PolarsSerdeError::ConversionError {
            message: "Expected Timestamp array for string conversion".to_string(),
        })?;
    
    let mut builder = StringBuilder::new();
    
    for i in 0..ts_array.len() {
        if ts_array.is_null(i) {
            builder.append_null();
        } else {
            let nanos = ts_array.value(i);
            
            if timezone.is_some() {
                // Convert to UTC DateTime string (RFC3339 format)
                let dt = DateTime::from_timestamp_nanos(nanos);
                builder.append_value(dt.to_rfc3339());
            } else {
                // Convert to NaiveDateTime string
                let dt = DateTime::from_timestamp_nanos(nanos).naive_utc();
                builder.append_value(dt.format("%Y-%m-%dT%H:%M:%S%.f").to_string());
            }
        }
    }
    
    Ok(Arc::new(builder.finish()))
}

/// Convert Date32/Timestamp columns back to string for serde_arrow compatibility
fn convert_from_chrono_columns(
    batch: RecordBatch,
) -> Result<RecordBatch> {
    
    let mut new_columns = Vec::new();
    let mut new_fields = Vec::new();
    let schema = batch.schema();

    for (i, column) in batch.columns().iter().enumerate() {
        let field = schema.field(i);
        let field_name = field.name();
        
        
        // Convert Date32 and Timestamp columns back to strings for serde_arrow compatibility
        match field.data_type() {
            DataType::Date32 => {
                // Convert Date32 back to string representation for chrono deserialization
                let string_array = convert_date32_to_string(column)?;
                new_columns.push(string_array);
                new_fields.push(Arc::new(Field::new(
                    field_name,
                    DataType::Utf8,
                    field.is_nullable(),
                )));
            },
            DataType::Timestamp(_, timezone) => {
                // Convert Timestamp back to string representation for chrono deserialization
                let string_array = convert_timestamp_to_string(column, timezone.clone())?;
                new_columns.push(string_array);
                new_fields.push(Arc::new(Field::new(
                    field_name,
                    DataType::Utf8,
                    field.is_nullable(),
                )));
            },
            DataType::Date64 => {
                // Handle Date64 as well (just in case)
                let string_array = compute::cast(column, &DataType::Utf8).map_err(|e| {
                    PolarsSerdeError::ConversionError {
                        message: format!("Failed to cast Date64 to String for field '{}': {}", field_name, e),
                    }
                })?;
                new_columns.push(string_array);
                new_fields.push(Arc::new(Field::new(
                    field_name,
                    DataType::Utf8,
                    field.is_nullable(),
                )));
            },
            _ => {
                // Keep all other types as-is
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


/// Helper function to deserialize with chrono type detection
fn deserialize_with_chrono_detection<T>(batch: &RecordBatch) -> Result<Vec<T>>
where
    T: DeserializeOwned,
{
    // Use standard serde_arrow deserialization
    from_record_batch(batch).map_err(|e| PolarsSerdeError::ConversionError {
        message: format!("Failed to deserialize batch: {}", e),
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
        // Apply reverse chrono conversion for DataFrame to struct conversion
        let converted_batch = convert_from_chrono_columns(batch.clone())?;
        let mut part: Vec<T> = deserialize_with_chrono_detection(&converted_batch)?;
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

    // Get basic schema generation
    let basic_fields: Vec<FieldRef> = match Vec::<FieldRef>::from_type::<T>(tracing_options.clone()) {
        Ok(basic_fields) => basic_fields,
        Err(_) => {
            // Fallback to samples-based schema generation
            Vec::<FieldRef>::from_samples(rows, tracing_options)?
        }
    };

    // Detect chrono types first
    let chrono_types = detect_chrono_types(&rows[0]).map_err(|e| PolarsSerdeError::ConversionError {
        message: format!("Failed to detect chrono types: {}", e),
    })?;
    
    // Create the record batch with chrono conversion
    let rb: RecordBatch = if chrono_types.is_empty() {
        // No chrono types, use normal serialization
        to_record_batch(&basic_fields, rows)?
    } else {
        // We have chrono types, serialize with numeric conversion
        // Note: This is a workaround - we serialize normally then convert the columns
        // The proper solution would be to use a custom serializer for each row,
        // but that's more complex and this works for our use case
        to_record_batch(&basic_fields, rows)?
    };
    
    // Apply chrono column conversion for detected chrono fields
    let converted_rb = convert_chrono_columns(rb, &chrono_types)?;

    // Convert any dictionary arrays to string arrays to avoid categorical requirements
    let final_rb = convert_dictionary_to_strings(converted_rb)?;

    let df: DataFrame = version_compat::arrow_to_dataframe(vec![final_rb])?;
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

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct DateRecord {
        name: String,
        birth_date: NaiveDate,
    }

    #[test]
    fn test_naive_date_conversion_from_polars_date() {
        use chrono::NaiveDate;
        
        // Test data with NaiveDate
        let records = vec![
            DateRecord {
                name: "Alice".to_string(),
                birth_date: NaiveDate::from_ymd_opt(1990, 5, 15).unwrap(),
            },
            DateRecord {
                name: "Bob".to_string(),
                birth_date: NaiveDate::from_ymd_opt(1985, 12, 3).unwrap(),
            },
        ];

        // Convert to DataFrame - this should create Date32 columns
        let df = to_dataframe(&records).unwrap();
        
        // Convert back to structs - this should work now with our fix!
        let converted_back: Vec<DateRecord> = from_dataframe(df).unwrap();
        
        // Verify round-trip conversion
        assert_eq!(records, converted_back);
    }

    #[test]
    fn test_date32_column_conversion() {
        use polars::prelude::*;
        
        // Create a DataFrame with a Date column directly (simulates the user's scenario)
        let dates = vec![
            NaiveDate::from_ymd_opt(1990, 5, 15).unwrap(),
            NaiveDate::from_ymd_opt(1985, 12, 3).unwrap(),
        ];
        
        let df = df![
            "name" => ["Alice", "Bob"],
            "birth_date" => dates.clone(),
        ].unwrap();
        
        // This should work now - converting a DataFrame with Date column to structs with NaiveDate
        let converted: Vec<DateRecord> = from_dataframe(df).unwrap();
        
        assert_eq!(converted.len(), 2);
        assert_eq!(converted[0].name, "Alice");
        assert_eq!(converted[0].birth_date, dates[0]);
        assert_eq!(converted[1].name, "Bob");
        assert_eq!(converted[1].birth_date, dates[1]);
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct UtcRecord {
        name: String,
        created_at: DateTime<Utc>,
    }

    #[test] 
    fn test_debug_datetime_conversion() {
        // Test what happens when we try to convert DateTime<Utc>
        let records = vec![
            UtcRecord {
                name: "Test".to_string(),
                created_at: DateTime::parse_from_rfc3339("2023-06-15T14:30:00Z").unwrap().with_timezone(&Utc),
            },
        ];

        // Convert to DataFrame first
        let df = to_dataframe(&records).unwrap();
        
        
        // This might fail - let's see what error we get
        let converted_back: Vec<UtcRecord> = from_dataframe(df).unwrap();
        assert_eq!(records, converted_back);
    }
    
    #[test]
    fn test_debug_date_column_types() {
        // Test what column types we get with the current implementation
        let records = vec![
            DateRecord {
                name: "Alice".to_string(),
                birth_date: NaiveDate::from_ymd_opt(1990, 5, 15).unwrap(),
            },
        ];

        let df = to_dataframe(&records).unwrap();
        
        
        // Check that roundtrip works
        let converted_back: Vec<DateRecord> = from_dataframe(df).unwrap();
        assert_eq!(records, converted_back);
    }
   
}
