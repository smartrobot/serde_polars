use chrono::{NaiveDate, NaiveDateTime, DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_polars::{from_dataframe, to_dataframe};

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
use polars_crate_0_46 as _polars;

#[cfg(feature = "polars_0_47")]
use polars_crate_0_47 as polars;

#[cfg(feature = "polars_0_48")]
use polars_crate_0_48 as polars;

#[cfg(feature = "polars_0_49")]
use polars_crate_0_49 as polars;

#[cfg(feature = "polars_0_50")]
use polars_crate_0_50 as polars;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct PersonWithDate {
    name: String,
    birth_date: NaiveDate,
    age: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct EventRecord {
    event_name: String,
    event_datetime: NaiveDateTime,
    description: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TimestampRecord {
    id: i64,
    created_at: DateTime<Utc>,
    value: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct MixedTemporalRecord {
    id: i64,
    name: String,
    birth_date: NaiveDate,
    last_login: NaiveDateTime,
    created_at: DateTime<Utc>,
    score: f64,
    active: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct OptionalDateRecord {
    id: i64,
    name: String,
    birth_date: Option<NaiveDate>,
    last_seen: Option<NaiveDateTime>,
}

#[test]
fn test_naive_date_roundtrip() {
    let records = vec![
        PersonWithDate {
            name: "Alice".to_string(),
            birth_date: NaiveDate::from_ymd_opt(1990, 5, 15).unwrap(),
            age: 33,
        },
        PersonWithDate {
            name: "Bob".to_string(),
            birth_date: NaiveDate::from_ymd_opt(1985, 12, 3).unwrap(),
            age: 38,
        },
        PersonWithDate {
            name: "Charlie".to_string(),
            birth_date: NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
            age: 23,
        },
    ];

    // Convert to DataFrame
    let df = to_dataframe(&records).unwrap();
    
    // Verify the DataFrame was created successfully
    assert_eq!(df.height(), 3);
    assert_eq!(df.width(), 3);
    
    // Convert back to structs
    let converted_back: Vec<PersonWithDate> = from_dataframe(df).unwrap();
    
    // Verify round-trip conversion
    assert_eq!(records, converted_back);
}

#[test]
fn test_naive_datetime_roundtrip() {
    let records = vec![
        EventRecord {
            event_name: "Meeting".to_string(),
            event_datetime: NaiveDate::from_ymd_opt(2023, 6, 15).unwrap()
                .and_hms_opt(14, 30, 0).unwrap(),
            description: "Team standup".to_string(),
        },
        EventRecord {
            event_name: "Lunch".to_string(),
            event_datetime: NaiveDate::from_ymd_opt(2023, 6, 15).unwrap()
                .and_hms_opt(12, 0, 0).unwrap(),
            description: "Team lunch".to_string(),
        },
    ];

    // Convert to DataFrame
    let df = to_dataframe(&records).unwrap();
    
    // Verify the DataFrame was created successfully
    assert_eq!(df.height(), 2);
    assert_eq!(df.width(), 3);
    
    // Convert back to structs
    let converted_back: Vec<EventRecord> = from_dataframe(df).unwrap();
    
    // Verify round-trip conversion
    assert_eq!(records, converted_back);
}

#[test]
fn test_datetime_utc_roundtrip() {
    let records = vec![
        TimestampRecord {
            id: 1,
            created_at: DateTime::parse_from_rfc3339("2023-06-15T14:30:00Z").unwrap().with_timezone(&Utc),
            value: 42.5,
        },
        TimestampRecord {
            id: 2,
            created_at: DateTime::parse_from_rfc3339("2023-06-16T09:15:30Z").unwrap().with_timezone(&Utc),
            value: 37.8,
        },
    ];

    // Convert to DataFrame
    let df = to_dataframe(&records).unwrap();
    
    // Verify the DataFrame was created successfully
    assert_eq!(df.height(), 2);
    assert_eq!(df.width(), 3);
    
    // Convert back to structs
    let converted_back: Vec<TimestampRecord> = from_dataframe(df).unwrap();
    
    // Verify round-trip conversion
    assert_eq!(records, converted_back);
}

#[test]
fn test_mixed_temporal_types() {
    let records = vec![
        MixedTemporalRecord {
            id: 1,
            name: "Alice".to_string(),
            birth_date: NaiveDate::from_ymd_opt(1990, 5, 15).unwrap(),
            last_login: NaiveDate::from_ymd_opt(2023, 6, 15).unwrap()
                .and_hms_opt(14, 30, 0).unwrap(),
            created_at: DateTime::parse_from_rfc3339("2023-01-01T00:00:00Z").unwrap().with_timezone(&Utc),
            score: 95.5,
            active: true,
        },
        MixedTemporalRecord {
            id: 2,
            name: "Bob".to_string(),
            birth_date: NaiveDate::from_ymd_opt(1985, 12, 3).unwrap(),
            last_login: NaiveDate::from_ymd_opt(2023, 6, 14).unwrap()
                .and_hms_opt(09, 15, 30).unwrap(),
            created_at: DateTime::parse_from_rfc3339("2022-12-15T10:30:00Z").unwrap().with_timezone(&Utc),
            score: 87.2,
            active: false,
        },
    ];

    // Convert to DataFrame
    let df = to_dataframe(&records).unwrap();
    
    // Verify the DataFrame was created successfully
    assert_eq!(df.height(), 2);
    assert_eq!(df.width(), 7);
    
    // Convert back to structs
    let converted_back: Vec<MixedTemporalRecord> = from_dataframe(df).unwrap();
    
    // Verify round-trip conversion
    assert_eq!(records, converted_back);
}

#[test]
fn test_optional_dates() {
    let records = vec![
        OptionalDateRecord {
            id: 1,
            name: "Alice".to_string(),
            birth_date: Some(NaiveDate::from_ymd_opt(1990, 5, 15).unwrap()),
            last_seen: Some(NaiveDate::from_ymd_opt(2023, 6, 15).unwrap()
                .and_hms_opt(14, 30, 0).unwrap()),
        },
        OptionalDateRecord {
            id: 2,
            name: "Bob".to_string(),
            birth_date: None,
            last_seen: None,
        },
        OptionalDateRecord {
            id: 3,
            name: "Charlie".to_string(),
            birth_date: Some(NaiveDate::from_ymd_opt(2000, 1, 1).unwrap()),
            last_seen: None,
        },
    ];

    // Convert to DataFrame
    let df = to_dataframe(&records).unwrap();
    
    // Verify the DataFrame was created successfully
    assert_eq!(df.height(), 3);
    assert_eq!(df.width(), 4);
    
    // Convert back to structs
    let converted_back: Vec<OptionalDateRecord> = from_dataframe(df).unwrap();
    
    // Verify round-trip conversion
    assert_eq!(records, converted_back);
}

#[test]
fn test_empty_date_vector() {
    let records: Vec<PersonWithDate> = vec![];
    
    // Should handle empty vector gracefully
    let result = to_dataframe(&records);
    assert!(result.is_err());
}

#[test]
fn test_single_date_record() {
    let records = vec![
        PersonWithDate {
            name: "Single".to_string(),
            birth_date: NaiveDate::from_ymd_opt(1995, 7, 20).unwrap(),
            age: 28,
        },
    ];

    // Convert to DataFrame
    let df = to_dataframe(&records).unwrap();
    
    // Verify the DataFrame was created successfully
    assert_eq!(df.height(), 1);
    assert_eq!(df.width(), 3);
    
    // Convert back to structs
    let converted_back: Vec<PersonWithDate> = from_dataframe(df).unwrap();
    
    // Verify round-trip conversion
    assert_eq!(records, converted_back);
}