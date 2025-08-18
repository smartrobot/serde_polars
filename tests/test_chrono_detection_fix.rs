use chrono::{NaiveDate, NaiveDateTime, DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_polars::{from_dataframe, to_dataframe};

// Test case: struct with mixed types including integer fields that should NOT be detected as chrono
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct MixedRecord {
    id: i64,
    calc_min_date: i32,  // This should NOT be converted as chrono - it's an integer!
    name: String,
    birth_date: NaiveDate,  // This SHOULD be converted
    score: f64,
    active: bool,
}

// Test case: struct with newtype wrappers that are NOT chrono types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct UserId(i64);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct CustomWrapper {
    user_id: UserId,  // This should NOT be detected as chrono
    calc_min_date: i32,  // This should NOT be detected as chrono  
    created_date: NaiveDate,  // This SHOULD be detected as chrono
}

#[test]
fn test_integer_fields_not_converted_as_chrono() {
    println!("Testing that integer fields are not incorrectly converted as chrono types...");
    
    let records = vec![
        MixedRecord {
            id: 1,
            calc_min_date: 20240101,  // This is an integer, not a date!
            name: "Test Record".to_string(),
            birth_date: NaiveDate::from_ymd_opt(1990, 5, 15).unwrap(),
            score: 95.5,
            active: true,
        },
        MixedRecord {
            id: 2,
            calc_min_date: 20231215,  // This is an integer, not a date!
            name: "Test Record 2".to_string(),
            birth_date: NaiveDate::from_ymd_opt(1985, 12, 3).unwrap(),
            score: 87.2,
            active: false,
        },
    ];
    
    println!("Original records: {:?}", records);
    
    // This should work - only birth_date should be detected as chrono, not calc_min_date
    let df = to_dataframe(&records).unwrap();
    println!("Successfully created DataFrame with {} rows", df.height());
    
    // This should also work - calc_min_date should remain as integer
    let converted_back: Vec<MixedRecord> = from_dataframe(df).unwrap();
    println!("Successfully converted back {} records", converted_back.len());
    
    // Verify the data is correct
    assert_eq!(records, converted_back);
    println!("✓ Round-trip conversion successful!");
}

#[test]
fn test_newtype_wrappers_not_converted_as_chrono() {
    println!("Testing that newtype wrappers are not incorrectly converted as chrono types...");
    
    let records = vec![
        CustomWrapper {
            user_id: UserId(12345),  // This should NOT be detected as chrono
            calc_min_date: 20240101,  // This should NOT be detected as chrono
            created_date: NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),  // This SHOULD be detected as chrono
        },
        CustomWrapper {
            user_id: UserId(67890),
            calc_min_date: 20231201,
            created_date: NaiveDate::from_ymd_opt(2023, 12, 1).unwrap(),
        },
    ];
    
    println!("Original records: {:?}", records);
    
    // This should work - only created_date should be detected as chrono
    let df = to_dataframe(&records).unwrap();
    println!("Successfully created DataFrame with {} rows", df.height());
    
    // This should also work
    let converted_back: Vec<CustomWrapper> = from_dataframe(df).unwrap();
    println!("Successfully converted back {} records", converted_back.len());
    
    // Verify the data is correct
    assert_eq!(records, converted_back);
    println!("✓ Newtype wrapper test successful!");
}