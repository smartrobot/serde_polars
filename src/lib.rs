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
//! - **Efficient date/time handling with numeric wrapper types**
//! - Support for both single structs and collections
//!
//! ## Efficient Date/Time Handling
//!
//! For maximum performance with dates and times, use the provided wrapper types:
//! - [`DateWrapper`] - Converts `NaiveDate` to/from i32 (days since Unix epoch)
//! - [`DateTimeWrapper`] - Converts `NaiveDateTime` to/from i64 (nanoseconds since Unix epoch)  
//! - [`UtcDateTimeWrapper`] - Converts `DateTime<Utc>` to/from i64 (nanoseconds since Unix epoch)
//!
//! **IMPORTANT**: Raw chrono types will serialize as strings. Use wrapper types for efficiency!
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
//! use serde_polars::{from_dataframe, to_dataframe, DateWrapper};
//! use chrono::NaiveDate;
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! struct Record {
//!     name: String,
//!     age: i32,
//!     birth_date: DateWrapper,  // Efficient i32 storage!
//!     score: f64,
//! }
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Create records with efficient date handling
//! let records = vec![
//!     Record {
//!         name: "Alice".to_string(),
//!         age: 25,
//!         birth_date: DateWrapper::new(NaiveDate::from_ymd_opt(1998, 5, 15).unwrap()),
//!         score: 85.5,
//!     },
//!     Record {
//!         name: "Bob".to_string(), 
//!         age: 30,
//!         birth_date: DateWrapper::new(NaiveDate::from_ymd_opt(1993, 8, 22).unwrap()),
//!         score: 92.0,
//!     },
//! ];
//!
//! // Convert to DataFrame (birth_date will be stored as i32 - no strings!)
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
use chrono::{NaiveDate, NaiveDateTime, DateTime, Utc, Duration};
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

/// High-performance numeric conversion wrappers for chrono types
/// 
/// IMPORTANT: These are the ONLY supported way to handle dates/times efficiently in this library!
/// Raw chrono types will serialize as strings - use these wrappers for maximum performance.

/// Wrapper for NaiveDate that converts to/from days since Unix epoch (i32)
/// This provides maximum efficiency for date storage in Polars DataFrames
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DateWrapper(pub NaiveDate);

impl DateWrapper {
    /// Create a new DateWrapper from a NaiveDate
    pub fn new(date: NaiveDate) -> Self {
        Self(date)
    }
    
    /// Get the underlying NaiveDate
    pub fn into_inner(self) -> NaiveDate {
        self.0
    }
}

impl serde::Serialize for DateWrapper {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Convert directly to days since Unix epoch (what Polars uses internally)
        let days = self.0.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32;
        serializer.serialize_newtype_struct("DateWrapper", &days)
    }
}

impl<'de> serde::Deserialize<'de> for DateWrapper {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let days = i32::deserialize(deserializer)?;
        let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(days as i64);
        Ok(DateWrapper(date))
    }
}

/// Wrapper for NaiveDateTime that converts to/from nanoseconds since Unix epoch (i64)
/// This provides maximum efficiency for datetime storage in Polars DataFrames
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DateTimeWrapper(pub NaiveDateTime);

impl DateTimeWrapper {
    /// Create a new DateTimeWrapper from a NaiveDateTime
    pub fn new(datetime: NaiveDateTime) -> Self {
        Self(datetime)
    }
    
    /// Get the underlying NaiveDateTime
    pub fn into_inner(self) -> NaiveDateTime {
        self.0
    }
}

impl serde::Serialize for DateTimeWrapper {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Convert directly to nanoseconds since Unix epoch
        let nanos = self.0.and_utc().timestamp_nanos_opt().unwrap_or(0);
        serializer.serialize_newtype_struct("DateTimeWrapper", &nanos)
    }
}

impl<'de> serde::Deserialize<'de> for DateTimeWrapper {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let nanos = i64::deserialize(deserializer)?;
        let dt = DateTime::from_timestamp_nanos(nanos).naive_utc();
        Ok(DateTimeWrapper(dt))
    }
}

/// Wrapper for DateTime<Utc> that converts to/from nanoseconds since Unix epoch (i64)
/// This provides maximum efficiency for UTC datetime storage in Polars DataFrames
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UtcDateTimeWrapper(pub DateTime<Utc>);

impl UtcDateTimeWrapper {
    /// Create a new UtcDateTimeWrapper from a DateTime<Utc>
    pub fn new(datetime: DateTime<Utc>) -> Self {
        Self(datetime)
    }
    
    /// Get the underlying DateTime<Utc>
    pub fn into_inner(self) -> DateTime<Utc> {
        self.0
    }
}

impl serde::Serialize for UtcDateTimeWrapper {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Convert directly to nanoseconds since Unix epoch
        let nanos = self.0.timestamp_nanos_opt().unwrap_or(0);
        serializer.serialize_newtype_struct("UtcDateTimeWrapper", &nanos)
    }
}

impl<'de> serde::Deserialize<'de> for UtcDateTimeWrapper {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let nanos = i64::deserialize(deserializer)?;
        let dt = DateTime::from_timestamp_nanos(nanos);
        Ok(UtcDateTimeWrapper(dt))
    }
}

use std::collections::HashMap;

/// Schema inspector that detects wrapper types during serialization
struct WrapperDetector {
    field_types: HashMap<String, String>,
    current_field: Option<String>,
}

impl WrapperDetector {
    fn new() -> Self {
        Self {
            field_types: HashMap::new(),
            current_field: None,
        }
    }
}

impl serde::ser::Serializer for &mut WrapperDetector {
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
    fn serialize_str(self, _v: &str) -> std::result::Result<Self::Ok, Self::Error> { Ok(()) }
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
        // This is where we detect our wrapper types!
        if let Some(field_name) = &self.current_field {
            self.field_types.insert(field_name.clone(), name.to_string());
        }
        value.serialize(self)
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
        impl serde::ser::$trait for &mut WrapperDetector {
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

impl serde::ser::SerializeMap for &mut WrapperDetector {
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

impl serde::ser::SerializeStruct for &mut WrapperDetector {
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

impl serde::ser::SerializeStructVariant for &mut WrapperDetector {
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

/// Detect wrapper types in a struct by examining serialization
fn detect_wrapper_types<T: Serialize>(sample: &T) -> std::result::Result<HashMap<String, String>, serde_arrow::Error> {
    let mut detector = WrapperDetector::new();
    sample.serialize(&mut detector)?;
    Ok(detector.field_types)
}

/// Convert detected wrapper fields to proper Arrow date types
fn enhance_schema_for_wrappers(
    mut fields: Vec<FieldRef>,
    wrapper_types: &HashMap<String, String>
) -> Vec<FieldRef> {
    for field_index in 0..fields.len() {
        let field = &fields[field_index];
        let field_name = field.name();
        
        if let Some(wrapper_type) = wrapper_types.get(field_name) {
            match wrapper_type.as_str() {
                "DateWrapper" => {
                    fields[field_index] = Arc::new(Field::new(
                        field_name,
                        DataType::Date32,
                        field.is_nullable()
                    ));
                },
                "DateTimeWrapper" | "UtcDateTimeWrapper" => {
                    fields[field_index] = Arc::new(Field::new(
                        field_name,
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        field.is_nullable()
                    ));
                },
                _ => {}
            }
        }
    }
    fields
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

    // Get basic schema generation
    let basic_fields: Vec<FieldRef> = match Vec::<FieldRef>::from_type::<T>(tracing_options.clone()) {
        Ok(basic_fields) => basic_fields,
        Err(_) => {
            // Fallback to samples-based schema generation
            Vec::<FieldRef>::from_samples(rows, tracing_options)?
        }
    };

    // Detect wrapper types and enhance schema  
    let wrapper_types = detect_wrapper_types(&rows[0]).map_err(|e| PolarsSerdeError::ConversionError {
        message: format!("Failed to detect wrapper types: {}", e),
    })?;
    
    let fields = enhance_schema_for_wrappers(basic_fields, &wrapper_types);

    let rb: RecordBatch = to_record_batch(&fields, rows)?;

    // Convert any dictionary arrays to string arrays to avoid categorical requirements
    let converted_rb = convert_dictionary_to_strings(rb)?;

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
