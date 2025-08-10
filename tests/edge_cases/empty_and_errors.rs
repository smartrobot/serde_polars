use serde::{Deserialize, Serialize};
use serde_polars::{from_dataframe, to_dataframe, PolarsSerdeError};

#[cfg(feature = "polars")]
use polars::prelude::*;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TestRecord {
    id: i64,
    name: String,
}

#[cfg(feature = "polars")]
#[test]
fn test_empty_vec_error() {
    let empty_records: Vec<TestRecord> = vec![];
    let result = to_dataframe(&empty_records);
    
    assert!(result.is_err());
    match result.unwrap_err() {
        PolarsSerdeError::EmptyInput => {}, // Expected
        other => panic!("Expected EmptyInput error, got: {:?}", other),
    }
}

#[cfg(feature = "polars")]
#[test] 
fn test_single_row_dataframe() {
    let record = TestRecord {
        id: 1,
        name: "Solo".to_string(),
    };
    
    let df = to_dataframe(&vec![record.clone()]).expect("Failed to create single-row DataFrame");
    assert_eq!(df.height(), 1);
    assert_eq!(df.width(), 2);
    
    let converted: Vec<TestRecord> = from_dataframe(df).expect("Failed to convert single-row back");
    assert_eq!(vec![record], converted);
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct EdgeCaseStrings {
    id: i64,
    empty_string: String,
    very_long_string: String,
    special_chars: String,
}

#[cfg(feature = "polars")]
#[test]
fn test_edge_case_strings() {
    let very_long_string = "a".repeat(10000);
    let special_chars = "!@#$%^&*()_+-=[]{}|;':\",./<>?`~\\";
    
    let records = vec![
        EdgeCaseStrings {
            id: 1,
            empty_string: "".to_string(),
            very_long_string: very_long_string.clone(),
            special_chars: special_chars.to_string(),
        },
        EdgeCaseStrings {
            id: 2,
            empty_string: "".to_string(),
            very_long_string: "short".to_string(),
            special_chars: "normal".to_string(),
        },
    ];

    let df = to_dataframe(&records).expect("Failed to convert edge case strings");
    let converted: Vec<EdgeCaseStrings> = from_dataframe(df).expect("Failed to convert back");
    
    assert_eq!(records, converted);
    assert_eq!(converted[0].very_long_string.len(), 10000);
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct NumericEdgeCases {
    max_i64: i64,
    min_i64: i64,
    max_u64: u64,
    min_u64: u64,
    max_f64: f64,
    min_positive_f64: f64,
    max_f32: f32,
    min_positive_f32: f32,
}

#[cfg(feature = "polars")]
#[test]
fn test_numeric_edge_cases() {
    let records = vec![
        NumericEdgeCases {
            max_i64: i64::MAX,
            min_i64: i64::MIN,
            max_u64: u64::MAX,
            min_u64: u64::MIN,
            max_f64: f64::MAX,
            min_positive_f64: f64::MIN_POSITIVE,
            max_f32: f32::MAX,
            min_positive_f32: f32::MIN_POSITIVE,
        },
    ];

    let df = to_dataframe(&records).expect("Failed to convert numeric edge cases");
    let converted: Vec<NumericEdgeCases> = from_dataframe(df).expect("Failed to convert back");
    
    assert_eq!(records, converted);
}