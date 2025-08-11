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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct BasicRecord {
    id: i64,
    name: String,
    score: f64,
    active: bool,
}

#[test]
fn test_basic_roundtrip() {
    let original_records = vec![
        BasicRecord {
            id: 1,
            name: "Alice".to_string(),
            score: 85.5,
            active: true,
        },
        BasicRecord {
            id: 2,
            name: "Bob".to_string(),
            score: 92.0,
            active: false,
        },
        BasicRecord {
            id: 3,
            name: "Charlie".to_string(),
            score: 78.3,
            active: true,
        },
    ];

    // Convert structs to DataFrame
    let df = to_dataframe(&original_records).expect("Failed to convert to DataFrame");
    
    // Verify DataFrame structure
    assert_eq!(df.width(), 4);
    assert_eq!(df.height(), 3);
    
    // Convert back to structs
    let converted_records: Vec<BasicRecord> = from_dataframe(df).expect("Failed to convert from DataFrame");
    
    // Verify roundtrip accuracy
    assert_eq!(original_records, converted_records);
}


#[test]
fn test_single_record() {
    let record = BasicRecord {
        id: 42,
        name: "Test".to_string(),
        score: 100.0,
        active: true,
    };
    
    let df = to_dataframe(&vec![record.clone()]).expect("Failed to convert single record");
    let converted: Vec<BasicRecord> = from_dataframe(df).expect("Failed to convert back");
    
    assert_eq!(vec![record], converted);
}

#[test]
fn test_large_dataset() {
    let large_records: Vec<BasicRecord> = (0..10000)
        .map(|i| BasicRecord {
            id: i,
            name: format!("User_{}", i),
            score: (i as f64) * 0.01,
            active: i % 2 == 0,
        })
        .collect();

    let df = to_dataframe(&large_records).expect("Failed to convert large dataset");
    assert_eq!(df.height(), 10000);
    
    let converted: Vec<BasicRecord> = from_dataframe(df).expect("Failed to convert large dataset back");
    assert_eq!(large_records, converted);
}
