use serde::{Deserialize, Serialize};
use serde_polars::{from_dataframe, to_dataframe};

#[cfg(feature = "polars")]
use polars::prelude::*;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct OptionalRecord {
    id: i64,
    name: Option<String>,
    score: Option<f64>,
    active: Option<bool>,
    count: Option<i32>,
}

#[cfg(feature = "polars")]
#[test]
fn test_optional_fields_mixed() {
    let records = vec![
        OptionalRecord {
            id: 1,
            name: Some("Alice".to_string()),
            score: Some(85.5),
            active: Some(true),
            count: Some(42),
        },
        OptionalRecord {
            id: 2,
            name: None,
            score: Some(92.0),
            active: None,
            count: Some(0),
        },
        OptionalRecord {
            id: 3,
            name: Some("Charlie".to_string()),
            score: None,
            active: Some(false),
            count: None,
        },
        OptionalRecord {
            id: 4,
            name: None,
            score: None,
            active: None,
            count: None,
        },
    ];

    let df = to_dataframe(&records).expect("Failed to convert optional fields");
    let converted: Vec<OptionalRecord> = from_dataframe(df).expect("Failed to convert back");
    
    assert_eq!(records, converted);
}

#[cfg(feature = "polars")]
#[test]
fn test_all_none_optional() {
    let records = vec![
        OptionalRecord {
            id: 1,
            name: None,
            score: None,
            active: None,
            count: None,
        },
        OptionalRecord {
            id: 2,
            name: None,
            score: None,
            active: None,
            count: None,
        },
    ];

    let df = to_dataframe(&records).expect("Failed to convert all-none records");
    let converted: Vec<OptionalRecord> = from_dataframe(df).expect("Failed to convert back");
    
    assert_eq!(records, converted);
}

#[cfg(feature = "polars")]
#[test]
fn test_all_some_optional() {
    let records = vec![
        OptionalRecord {
            id: 1,
            name: Some("Test1".to_string()),
            score: Some(100.0),
            active: Some(true),
            count: Some(1),
        },
        OptionalRecord {
            id: 2,
            name: Some("Test2".to_string()),
            score: Some(200.0),
            active: Some(false),
            count: Some(2),
        },
    ];

    let df = to_dataframe(&records).expect("Failed to convert all-some records");
    let converted: Vec<OptionalRecord> = from_dataframe(df).expect("Failed to convert back");
    
    assert_eq!(records, converted);
}