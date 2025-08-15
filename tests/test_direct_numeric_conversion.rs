use chrono::{NaiveDate, DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_polars::{to_dataframe, from_dataframe, DateWrapper, DateTimeWrapper, UtcDateTimeWrapper};

// Import DataType for tests
#[cfg(feature = "polars_0_40")]
use polars_crate_0_40::datatypes::DataType;

#[cfg(feature = "polars_0_41")]
use polars_crate_0_41::datatypes::DataType;

#[cfg(feature = "polars_0_42")]
use polars_crate_0_42::datatypes::DataType;

#[cfg(feature = "polars_0_43")]
use polars_crate_0_43::datatypes::DataType;

#[cfg(feature = "polars_0_44")]
use polars_crate_0_44::datatypes::DataType;

#[cfg(feature = "polars_0_45")]
use polars_crate_0_45::datatypes::DataType;

#[cfg(feature = "polars_0_46")]
use polars_crate_0_46::datatypes::DataType;

#[cfg(feature = "polars_0_47")]
use polars_crate_0_47::datatypes::DataType;

#[cfg(feature = "polars_0_48")]
use polars_crate_0_48::datatypes::DataType;

#[cfg(feature = "polars_0_49")]
use polars_crate_0_49::datatypes::DataType;

#[cfg(feature = "polars_0_50")]
use polars_crate_0_50::datatypes::DataType;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct DirectDateRecord {
    id: i32,
    birth_date: DateWrapper,  // Direct i32 conversion
    name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct DirectDateTimeRecord {
    id: i32,
    event_time: DateTimeWrapper,  // Direct i64 conversion
    description: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct DirectUtcRecord {
    id: i32,
    created_at: UtcDateTimeWrapper,  // Direct i64 conversion
    value: f64,
}

#[test]
fn test_direct_date_conversion() {
    let records = vec![
        DirectDateRecord {
            id: 1,
            birth_date: DateWrapper::new(NaiveDate::from_ymd_opt(1990, 5, 15).unwrap()),
            name: "Alice".to_string(),
        },
        DirectDateRecord {
            id: 2,
            birth_date: DateWrapper::new(NaiveDate::from_ymd_opt(1985, 12, 3).unwrap()),
            name: "Bob".to_string(),
        },
    ];

    // Convert to DataFrame - should use i32 directly, no string conversion!
    let df = to_dataframe(&records).unwrap();
    
    println!("Direct conversion schema:");
    for (name, dtype) in df.schema().iter() {
        println!("  {}: {:?}", name, dtype);
    }
    
    // NOW we should get proper Date types thanks to wrapper detection!
    let birth_date_column = df.column("birth_date").unwrap();
    assert_eq!(birth_date_column.dtype(), &DataType::Date);
    
    // Round-trip test should still work
    let converted_back: Vec<DirectDateRecord> = from_dataframe(df).unwrap();
    assert_eq!(records, converted_back);
    
    // Victory: Direct numeric storage + proper Polars date type!
}

#[test]
fn test_direct_datetime_conversion() {
    let records = vec![
        DirectDateTimeRecord {
            id: 1,
            event_time: DateTimeWrapper::new(
                NaiveDate::from_ymd_opt(2023, 6, 15).unwrap()
                    .and_hms_opt(14, 30, 0).unwrap()
            ),
            description: "Meeting".to_string(),
        },
    ];

    let df = to_dataframe(&records).unwrap();
    
    println!("Direct datetime schema:");
    for (name, dtype) in df.schema().iter() {
        println!("  {}: {:?}", name, dtype);
    }
    
    // Should now be proper Datetime type thanks to wrapper detection!
    let datetime_column = df.column("event_time").unwrap();
    assert!(matches!(datetime_column.dtype(), DataType::Datetime(_, _)));
    
    let converted_back: Vec<DirectDateTimeRecord> = from_dataframe(df).unwrap();
    assert_eq!(records, converted_back);
}

#[test]
fn test_direct_utc_conversion() {
    let records = vec![
        DirectUtcRecord {
            id: 1,
            created_at: UtcDateTimeWrapper::new(
                DateTime::parse_from_rfc3339("2023-06-15T14:30:00Z").unwrap().with_timezone(&Utc)
            ),
            value: 42.5,
        },
    ];

    let df = to_dataframe(&records).unwrap();
    
    println!("Direct UTC schema:");
    for (name, dtype) in df.schema().iter() {
        println!("  {}: {:?}", name, dtype);
    }
    
    // Should now be proper Datetime type thanks to wrapper detection!
    let datetime_column = df.column("created_at").unwrap();
    assert!(matches!(datetime_column.dtype(), DataType::Datetime(_, _)));
    
    let converted_back: Vec<DirectUtcRecord> = from_dataframe(df).unwrap();
    assert_eq!(records, converted_back);
}