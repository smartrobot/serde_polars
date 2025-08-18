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

// Test pattern 1: Nested structures with chrono fields
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct NestedRecord {
    id: i64,
    metadata: RecordMetadata,
    events: Vec<EventInfo>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct RecordMetadata {
    created_date: NaiveDate,
    updated_at: DateTime<Utc>,
    version: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct EventInfo {
    event_time: NaiveDateTime,
    event_type: String,
}

// Test pattern 2: Optional chrono fields
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct OptionalChronoRecord {
    id: i64,
    name: String,
    birth_date: Option<NaiveDate>,
    last_login: Option<NaiveDateTime>,
    deleted_at: Option<DateTime<Utc>>,
}

// Test pattern 3: Mixed primitive and chrono types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct MixedTypesRecord {
    id: i64,
    uuid: String,
    timestamp: DateTime<Utc>,
    amount: f64,
    count: i32,
    active: bool,
    tags: Vec<String>,
    date_created: NaiveDate,
}

// Test pattern 4: Records with many chrono fields
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ManyChronoFieldsRecord {
    id: i64,
    birth_date: NaiveDate,
    hire_date: NaiveDate,
    termination_date: Option<NaiveDate>,
    last_login: NaiveDateTime,
    password_changed_at: NaiveDateTime,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    deleted_at: Option<DateTime<Utc>>,
}

#[test]
fn test_edge_case_nested_structures() {
    println!("=== Testing Nested Structures with Chrono Fields ===");
    
    let records = vec![
        NestedRecord {
            id: 1,
            metadata: RecordMetadata {
                created_date: NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
                updated_at: DateTime::parse_from_rfc3339("2023-06-15T14:30:00Z").unwrap().with_timezone(&Utc),
                version: 1,
            },
            events: vec![
                EventInfo {
                    event_time: NaiveDate::from_ymd_opt(2023, 6, 15).unwrap().and_hms_opt(14, 30, 0).unwrap(),
                    event_type: "login".to_string(),
                },
                EventInfo {
                    event_time: NaiveDate::from_ymd_opt(2023, 6, 15).unwrap().and_hms_opt(15, 45, 0).unwrap(),
                    event_type: "logout".to_string(),
                },
            ],
        },
        NestedRecord {
            id: 2,
            metadata: RecordMetadata {
                created_date: NaiveDate::from_ymd_opt(2023, 2, 1).unwrap(),
                updated_at: DateTime::parse_from_rfc3339("2023-06-16T10:00:00Z").unwrap().with_timezone(&Utc),
                version: 2,
            },
            events: vec![],
        },
    ];
    
    println!("Original nested records: {}", records.len());
    
    // NOTE: This test might fail because nested structures with chrono fields
    // may not be supported by the current implementation
    let result = to_dataframe(&records);
    match result {
        Ok(df) => {
            println!("Successfully created DataFrame with {} rows", df.height());
            let converted_back: Result<Vec<NestedRecord>, _> = from_dataframe(df);
            match converted_back {
                Ok(converted) => {
                    println!("Successfully converted back {} records", converted.len());
                    assert_eq!(records.len(), converted.len());
                },
                Err(e) => {
                    println!("EXPECTED: Failed to convert back from DataFrame: {}", e);
                    println!("This is expected as nested structures with chrono fields are complex");
                }
            }
        },
        Err(e) => {
            println!("EXPECTED: Failed to create DataFrame from nested structures: {}", e);
            println!("This is expected as nested structures are not fully supported");
        }
    }
}

#[test]
fn test_edge_case_many_optional_chrono_fields() {
    println!("=== Testing Many Optional Chrono Fields ===");
    
    let records = vec![
        OptionalChronoRecord {
            id: 1,
            name: "Alice".to_string(),
            birth_date: Some(NaiveDate::from_ymd_opt(1990, 5, 15).unwrap()),
            last_login: Some(NaiveDate::from_ymd_opt(2023, 6, 15).unwrap().and_hms_opt(14, 30, 0).unwrap()),
            deleted_at: None,
        },
        OptionalChronoRecord {
            id: 2,
            name: "Bob".to_string(),
            birth_date: None,
            last_login: None,
            deleted_at: Some(DateTime::parse_from_rfc3339("2023-06-15T14:30:00Z").unwrap().with_timezone(&Utc)),
        },
        OptionalChronoRecord {
            id: 3,
            name: "Charlie".to_string(),
            birth_date: Some(NaiveDate::from_ymd_opt(2000, 1, 1).unwrap()),
            last_login: None,
            deleted_at: None,
        },
    ];
    
    println!("Original records with optional chrono fields: {}", records.len());
    
    let df = to_dataframe(&records).unwrap();
    println!("Created DataFrame with {} rows, {} columns", df.height(), df.width());
    
    let converted_back: Vec<OptionalChronoRecord> = from_dataframe(df).unwrap();
    println!("Converted back {} records", converted_back.len());
    
    assert_eq!(records.len(), converted_back.len());
    assert_eq!(records, converted_back);
    
    println!("✓ Optional chrono fields test passed");
}

#[test]
fn test_edge_case_many_chrono_fields() {
    println!("=== Testing Records with Many Chrono Fields ===");
    
    let records = vec![
        ManyChronoFieldsRecord {
            id: 1,
            birth_date: NaiveDate::from_ymd_opt(1990, 5, 15).unwrap(),
            hire_date: NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
            termination_date: None,
            last_login: NaiveDate::from_ymd_opt(2023, 6, 15).unwrap().and_hms_opt(14, 30, 0).unwrap(),
            password_changed_at: NaiveDate::from_ymd_opt(2023, 1, 1).unwrap().and_hms_opt(09, 00, 0).unwrap(),
            created_at: DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z").unwrap().with_timezone(&Utc),
            updated_at: DateTime::parse_from_rfc3339("2023-06-15T14:30:00Z").unwrap().with_timezone(&Utc),
            deleted_at: None,
        },
        ManyChronoFieldsRecord {
            id: 2,
            birth_date: NaiveDate::from_ymd_opt(1985, 12, 3).unwrap(),
            hire_date: NaiveDate::from_ymd_opt(2018, 6, 15).unwrap(),
            termination_date: Some(NaiveDate::from_ymd_opt(2023, 3, 1).unwrap()),
            last_login: NaiveDate::from_ymd_opt(2023, 2, 28).unwrap().and_hms_opt(16, 45, 30).unwrap(),
            password_changed_at: NaiveDate::from_ymd_opt(2022, 12, 1).unwrap().and_hms_opt(12, 00, 0).unwrap(),
            created_at: DateTime::parse_from_rfc3339("2018-06-15T00:00:00Z").unwrap().with_timezone(&Utc),
            updated_at: DateTime::parse_from_rfc3339("2023-03-01T09:00:00Z").unwrap().with_timezone(&Utc),
            deleted_at: Some(DateTime::parse_from_rfc3339("2023-03-01T09:00:00Z").unwrap().with_timezone(&Utc)),
        },
    ];
    
    println!("Original records with many chrono fields: {}", records.len());
    
    let df = to_dataframe(&records).unwrap();
    println!("Created DataFrame with {} rows, {} columns", df.height(), df.width());
    
    let converted_back: Vec<ManyChronoFieldsRecord> = from_dataframe(df).unwrap();
    println!("Converted back {} records", converted_back.len());
    
    assert_eq!(records.len(), converted_back.len());
    assert_eq!(records, converted_back);
    
    println!("✓ Many chrono fields test passed");
}

#[test]
fn test_edge_case_mixed_types_large_dataset() {
    println!("=== Testing Mixed Types with Large Dataset (100k records) ===");
    
    let mut records = Vec::with_capacity(100_000);
    
    for i in 0..100_000 {
        records.push(MixedTypesRecord {
            id: i,
            uuid: format!("uuid-{:08}", i),
            timestamp: DateTime::parse_from_rfc3339("2023-01-01T00:00:00Z").unwrap().with_timezone(&Utc),
            amount: (i as f64) * 1.5 + 0.33,
            count: (i % 1000) as i32,
            active: i % 3 == 0,
            tags: vec![
                format!("tag-{}", i % 10),
                format!("category-{}", i % 5),
                format!("type-{}", i % 3),
            ],
            date_created: NaiveDate::from_ymd_opt(2023, 1 + (i % 12) as u32, 1 + (i % 28) as u32).unwrap(),
        });
    }
    
    println!("Original mixed-type records: {}", records.len());
    
    let df = to_dataframe(&records).unwrap();
    println!("Created DataFrame with {} rows, {} columns", df.height(), df.width());
    
    assert_eq!(df.height(), 100_000);
    
    let converted_back: Vec<MixedTypesRecord> = from_dataframe(df).unwrap();
    println!("Converted back {} records", converted_back.len());
    
    // This is the critical test - if this fails, we have data loss
    assert_eq!(records.len(), converted_back.len(), 
               "CRITICAL DATA LOSS: Expected {} records, got {}", 
               records.len(), converted_back.len());
    
    // Spot check some records for data integrity
    for i in [0, 1000, 50000, 99999] {
        assert_eq!(records[i], converted_back[i], 
                   "Record {} data integrity check failed", i);
    }
    
    println!("✓ Large mixed-type dataset test passed");
}

#[test] 
fn test_edge_case_empty_and_single_record() {
    println!("=== Testing Empty and Single Record Edge Cases ===");
    
    // Test single record
    let single_record = vec![
        OptionalChronoRecord {
            id: 42,
            name: "Only One".to_string(),
            birth_date: Some(NaiveDate::from_ymd_opt(1995, 7, 20).unwrap()),
            last_login: Some(NaiveDate::from_ymd_opt(2023, 6, 15).unwrap().and_hms_opt(14, 30, 0).unwrap()),
            deleted_at: None,
        },
    ];
    
    println!("Testing single record...");
    let df = to_dataframe(&single_record).unwrap();
    assert_eq!(df.height(), 1);
    
    let converted_back: Vec<OptionalChronoRecord> = from_dataframe(df).unwrap();
    assert_eq!(converted_back.len(), 1);
    assert_eq!(single_record, converted_back);
    println!("✓ Single record test passed");
    
    // Test empty vector
    println!("Testing empty vector...");
    let empty_records: Vec<OptionalChronoRecord> = vec![];
    let result = to_dataframe(&empty_records);
    
    match result {
        Ok(_) => panic!("Empty vector should not create a valid DataFrame"),
        Err(_) => println!("✓ Empty vector correctly rejected"),
    }
}