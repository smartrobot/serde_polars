use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use serde_polars::from_dataframe;

#[cfg(feature = "polars_0_50")]
use polars_crate_0_50 as polars;

use polars::prelude::*;

// This struct expects calc_min_date as an integer, NOT a chrono type
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TestRecord {
    id: i64,
    calc_min_date: i32,  // This should be deserialized as an integer, not converted from a date
    name: String,
}

#[test]
fn test_dataframe_with_date_to_struct_with_int() {
    println!("=== Testing DataFrame with Date column → Struct with integer field ===");
    
    // Create a DataFrame with an integer column (simulating your scenario after date→int conversion)
    let df_with_int = df! {
        "id" => [1i64, 2i64, 3i64],
        "calc_min_date" => [20240101i32, 20231215i32, 20231130i32],  // Integer values
        "name" => ["Record 1", "Record 2", "Record 3"],
    }.unwrap();
    
    println!("\nDataFrame after converting date to int32:");
    for (name, dtype) in df_with_int.schema().iter() {
        println!("  {}: {:?}", name, dtype);
    }
    
    println!("\nDataFrame content:");
    println!("{}", df_with_int);
    
    // This is where the error occurs - trying to convert DataFrame to Vec<TestRecord>
    println!("\nAttempting from_dataframe conversion...");
    
    match from_dataframe::<TestRecord>(df_with_int) {
        Ok(records) => {
            println!("✓ Successfully converted to {} records", records.len());
            for record in &records {
                println!("  {:?}", record);
            }
        },
        Err(e) => {
            println!("✗ Error in from_dataframe: {}", e);
            println!("This is the bug - the library is trying to convert integer columns as if they were chrono types!");
        }
    }
}

#[test]
fn test_original_date_dataframe_to_struct() {
    println!("=== Testing DataFrame with original Date column → Struct with integer field ===");
    
    // Create a DataFrame with a Date column (original date data)
    let df = df! {
        "id" => [1i64, 2i64, 3i64],
        "calc_min_date" => [
            Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
            Some(NaiveDate::from_ymd_opt(2023, 12, 15).unwrap()),
            Some(NaiveDate::from_ymd_opt(2023, 11, 30).unwrap()),
        ],
        "name" => ["Record 1", "Record 2", "Record 3"],
    }.unwrap();
    
    println!("DataFrame with Date column schema:");
    for (name, dtype) in df.schema().iter() {
        println!("  {}: {:?}", name, dtype);
    }
    
    // Try to convert directly - this should fail because calc_min_date is Date but struct expects i32
    println!("\nAttempting from_dataframe conversion with Date column...");
    
    match from_dataframe::<TestRecord>(df) {
        Ok(records) => {
            println!("✓ Unexpectedly succeeded with {} records", records.len());
            for record in &records {
                println!("  {:?}", record);
            }
        },
        Err(e) => {
            println!("✗ Expected error: {}", e);
            println!("This shows the library incorrectly trying to handle Date→integer conversion");
        }
    }
}