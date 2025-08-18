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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct LargeDataRecord {
    id: i64,
    name: String,
    birth_date: NaiveDate,
    last_login: NaiveDateTime,
    created_at: DateTime<Utc>,
    score: f64,
    active: bool,
}

#[test]
fn test_large_dataset_roundtrip() {
    println!("Creating large dataset with 200,000 records...");
    
    let mut records = Vec::with_capacity(200_000);
    
    for i in 0..200_000 {
        records.push(LargeDataRecord {
            id: i,
            name: format!("User_{}", i),
            birth_date: NaiveDate::from_ymd_opt(1990 + (i % 30) as i32, 1 + (i % 12) as u32, 1 + (i % 28) as u32).unwrap(),
            last_login: NaiveDate::from_ymd_opt(2023, 6, 15).unwrap()
                .and_hms_opt(14, 30, (i % 60) as u32).unwrap(),
            created_at: DateTime::parse_from_rfc3339("2023-01-01T00:00:00Z").unwrap().with_timezone(&Utc),
            score: 50.0 + (i as f64 % 100.0),
            active: i % 2 == 0,
        });
    }
    
    println!("Original dataset size: {} records", records.len());
    
    // Convert to DataFrame
    println!("Converting to DataFrame...");
    let df = to_dataframe(&records).unwrap();
    
    println!("DataFrame created with {} rows and {} columns", df.height(), df.width());
    assert_eq!(df.height(), 200_000);
    
    // Convert back to structs
    println!("Converting back from DataFrame...");
    let converted_back: Vec<LargeDataRecord> = from_dataframe(df).unwrap();
    
    println!("Converted back dataset size: {} records", converted_back.len());
    
    // This is where the bug manifests - we should get 200k records back, not 0
    assert_eq!(converted_back.len(), 200_000, "Data loss detected! Expected 200,000 records, got {}", converted_back.len());
    
    // Verify a few sample records for correctness
    if !converted_back.is_empty() {
        assert_eq!(records[0], converted_back[0]);
        assert_eq!(records[100_000], converted_back[100_000]);
        assert_eq!(records[199_999], converted_back[199_999]);
    }
    
    println!("Round-trip test completed successfully!");
}

#[test] 
fn test_medium_dataset_roundtrip() {
    println!("Creating medium dataset with 10,000 records...");
    
    let mut records = Vec::with_capacity(10_000);
    
    for i in 0..10_000 {
        records.push(LargeDataRecord {
            id: i,
            name: format!("User_{}", i),
            birth_date: NaiveDate::from_ymd_opt(1990 + (i % 30) as i32, 1 + (i % 12) as u32, 1 + (i % 28) as u32).unwrap(),
            last_login: NaiveDate::from_ymd_opt(2023, 6, 15).unwrap()
                .and_hms_opt(14, 30, (i % 60) as u32).unwrap(),
            created_at: DateTime::parse_from_rfc3339("2023-01-01T00:00:00Z").unwrap().with_timezone(&Utc),
            score: 50.0 + (i as f64 % 100.0),
            active: i % 2 == 0,
        });
    }
    
    println!("Original dataset size: {} records", records.len());
    
    // Convert to DataFrame
    println!("Converting to DataFrame...");
    let df = to_dataframe(&records).unwrap();
    
    println!("DataFrame created with {} rows and {} columns", df.height(), df.width());
    assert_eq!(df.height(), 10_000);
    
    // Convert back to structs
    println!("Converting back from DataFrame...");
    let converted_back: Vec<LargeDataRecord> = from_dataframe(df).unwrap();
    
    println!("Converted back dataset size: {} records", converted_back.len());
    
    // Check for data loss
    assert_eq!(converted_back.len(), 10_000, "Data loss detected! Expected 10,000 records, got {}", converted_back.len());
    
    println!("Medium dataset test completed successfully!");
}