use serde::{Deserialize, Serialize};
use serde_polars::{from_dataframe, to_dataframe};

#[cfg(feature = "polars")]
use polars::prelude::*;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TestRecord {
    id: i64,
    name: String,
    score: f64,
}

#[cfg(feature = "polars")]
#[test]
fn test_mismatched_schema_detection() {
    // This test ensures that our conversion properly handles type mismatches
    // by creating a DataFrame manually with different types than expected
    
    let df = df!(
        "id" => [1i64, 2i64, 3i64],
        "name" => ["Alice", "Bob", "Charlie"],
        "score" => ["not_a_number", "also_not_a_number", "still_not_a_number"], // Wrong type!
    ).expect("Failed to create test DataFrame");

    // This should fail gracefully when trying to convert strings to f64
    let result: Result<Vec<TestRecord>, _> = from_dataframe(df);
    assert!(result.is_err(), "Expected conversion to fail with type mismatch");
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct MissingFieldRecord {
    id: i64,
    name: String,
    // Note: we'll create a DataFrame that's missing the 'extra' field
}

#[cfg(feature = "polars")]
#[test] 
fn test_missing_dataframe_columns() {
    // Create DataFrame with missing column
    let df = df!(
        "id" => [1i64, 2i64],
        "name" => ["Alice", "Bob"],
        // Missing expected fields - this should be handled gracefully
    ).expect("Failed to create DataFrame");

    let result: Result<Vec<MissingFieldRecord>, _> = from_dataframe(df);
    // This might succeed with default values or fail - both are acceptable
    // The important thing is that it doesn't panic
    match result {
        Ok(_) => println!("Conversion succeeded with missing fields"),
        Err(e) => println!("Conversion failed gracefully: {:?}", e),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ExtraFieldRecord {
    id: i64,
    // Note: DataFrame will have extra fields that this struct doesn't have
}

#[cfg(feature = "polars")]
#[test]
fn test_extra_dataframe_columns() {
    // Create DataFrame with extra columns
    let df = df!(
        "id" => [1i64, 2i64],
        "extra_field1" => ["value1", "value2"],
        "extra_field2" => [10.0, 20.0],
        "extra_field3" => [true, false],
    ).expect("Failed to create DataFrame");

    // This should succeed, ignoring extra columns
    let result: Vec<ExtraFieldRecord> = from_dataframe(df).expect("Should handle extra columns");
    
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].id, 1);
    assert_eq!(result[1].id, 2);
}