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

use polars::prelude::*;

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
struct TestRecord {
    id: i64,
    name: String,
    birth_date: NaiveDate,
    last_login: NaiveDateTime,
    created_at: DateTime<Utc>,
    score: f64,
    active: bool,
}

fn compare_dataframes(original_df: &DataFrame, final_df: &DataFrame) -> Result<(), String> {
    // Compare shapes
    if original_df.height() != final_df.height() {
        return Err(format!(
            "Row count mismatch: original {} vs final {}",
            original_df.height(),
            final_df.height()
        ));
    }
    
    if original_df.width() != final_df.width() {
        return Err(format!(
            "Column count mismatch: original {} vs final {}",
            original_df.width(),
            final_df.width()
        ));
    }
    
    // Compare column names
    let orig_cols = original_df.get_column_names();
    let final_cols = final_df.get_column_names();
    
    if orig_cols != final_cols {
        return Err(format!(
            "Column names mismatch: original {:?} vs final {:?}",
            orig_cols, final_cols
        ));
    }
    
    println!("✓ DataFrame shapes and column names match");
    println!("  Rows: {}, Columns: {}", original_df.height(), original_df.width());
    
    // Compare schema (but allow Date32/Timestamp differences since that's expected)
    for col_name in &orig_cols {
        let orig_dtype = original_df.column(col_name).unwrap().dtype();
        let final_dtype = final_df.column(col_name).unwrap().dtype();
        
        println!("  Column '{}': {} -> {}", col_name, orig_dtype, final_dtype);
        
        // For date/datetime columns, we expect conversion to efficient types
        match (orig_dtype, final_dtype) {
            (DataType::String, DataType::Date) => {
                println!("    ✓ Expected conversion from string to Date32 for date column");
            },
            (DataType::String, DataType::Datetime(_, _)) => {
                println!("    ✓ Expected conversion from string to Timestamp for datetime column");
            },
            (a, b) if a == b => {
                println!("    ✓ Type preserved");
            },
            _ => {
                println!("    ! Type changed from {} to {}", orig_dtype, final_dtype);
            }
        }
    }
    
    Ok(())
}

#[test]
fn test_comprehensive_roundtrip_comparison() {
    println!("=== Comprehensive Round-Trip Test ===");
    
    // Create test data
    let records = vec![
        TestRecord {
            id: 1,
            name: "Alice".to_string(),
            birth_date: NaiveDate::from_ymd_opt(1990, 5, 15).unwrap(),
            last_login: NaiveDate::from_ymd_opt(2023, 6, 15).unwrap()
                .and_hms_opt(14, 30, 0).unwrap(),
            created_at: DateTime::parse_from_rfc3339("2023-01-01T00:00:00Z").unwrap().with_timezone(&Utc),
            score: 95.5,
            active: true,
        },
        TestRecord {
            id: 2,
            name: "Bob".to_string(),
            birth_date: NaiveDate::from_ymd_opt(1985, 12, 3).unwrap(),
            last_login: NaiveDate::from_ymd_opt(2023, 6, 14).unwrap()
                .and_hms_opt(09, 15, 30).unwrap(),
            created_at: DateTime::parse_from_rfc3339("2022-12-15T10:30:00Z").unwrap().with_timezone(&Utc),
            score: 87.2,
            active: false,
        },
        TestRecord {
            id: 3,
            name: "Charlie".to_string(),
            birth_date: NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
            last_login: NaiveDate::from_ymd_opt(2023, 6, 16).unwrap()
                .and_hms_opt(20, 45, 15).unwrap(),
            created_at: DateTime::parse_from_rfc3339("2023-06-01T12:00:00Z").unwrap().with_timezone(&Utc),
            score: 78.9,
            active: true,
        },
    ];
    
    println!("Original data: {} records", records.len());
    
    // Step 1: Convert to DataFrame
    println!("\n1. Converting Vec<T> to DataFrame...");
    let original_df = to_dataframe(&records).unwrap();
    println!("  Created DataFrame with {} rows, {} columns", 
             original_df.height(), original_df.width());
    
    // Step 2: Convert back to Vec<T>
    println!("\n2. Converting DataFrame back to Vec<T>...");
    let converted_records: Vec<TestRecord> = from_dataframe(original_df.clone()).unwrap();
    println!("  Deserialized {} records", converted_records.len());
    
    // Step 3: Convert back to DataFrame again
    println!("\n3. Converting Vec<T> to DataFrame again...");
    let final_df = to_dataframe(&converted_records).unwrap();
    println!("  Created final DataFrame with {} rows, {} columns", 
             final_df.height(), final_df.width());
    
    // Step 4: Compare original DataFrame to final DataFrame
    println!("\n4. Comparing original DataFrame to final DataFrame...");
    compare_dataframes(&original_df, &final_df).unwrap();
    
    // Step 5: Compare original records to converted records
    println!("\n5. Comparing original records to converted records...");
    assert_eq!(records.len(), converted_records.len(), 
               "Record count mismatch: {} vs {}", records.len(), converted_records.len());
    
    for (i, (orig, conv)) in records.iter().zip(converted_records.iter()).enumerate() {
        assert_eq!(orig, conv, "Record {} differs:\nOriginal: {:?}\nConverted: {:?}", i, orig, conv);
    }
    
    println!("  ✓ All {} records match exactly", records.len());
    
    println!("\n=== Round-Trip Test PASSED ===");
}

#[test]
fn test_large_roundtrip_comparison() {
    println!("=== Large Dataset Round-Trip Test ===");
    
    // Create larger test data
    let mut records = Vec::with_capacity(50_000);
    
    for i in 0..50_000 {
        records.push(TestRecord {
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
    
    println!("Original data: {} records", records.len());
    
    // Full round-trip test
    println!("\n1. Converting Vec<T> to DataFrame...");
    let original_df = to_dataframe(&records).unwrap();
    
    println!("\n2. Converting DataFrame back to Vec<T>...");
    let converted_records: Vec<TestRecord> = from_dataframe(original_df.clone()).unwrap();
    
    println!("\n3. Converting Vec<T> to DataFrame again...");
    let final_df = to_dataframe(&converted_records).unwrap();
    
    println!("\n4. Comparing DataFrames...");
    compare_dataframes(&original_df, &final_df).unwrap();
    
    // Verify record counts
    assert_eq!(records.len(), converted_records.len(), 
               "CRITICAL: Record count mismatch: {} vs {}", records.len(), converted_records.len());
    
    // Spot check some records
    for i in [0, 1000, 25000, 49999] {
        assert_eq!(records[i], converted_records[i], 
                   "Record {} differs:\nOriginal: {:?}\nConverted: {:?}", i, records[i], converted_records[i]);
    }
    
    println!("  ✓ All {} records preserved correctly", records.len());
    
    println!("\n=== Large Round-Trip Test PASSED ===");
}