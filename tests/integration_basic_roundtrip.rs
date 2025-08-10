use serde::{Deserialize, Serialize};
use serde_polars::{from_dataframe, to_dataframe};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct BasicRecord {
    id: i64,
    name: String,
    score: f64,
    active: bool,
}

#[cfg(feature = "polars")]
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
    let converted_records: Vec<BasicRecord> =
        from_dataframe(df).expect("Failed to convert from DataFrame");

    // Verify roundtrip accuracy
    assert_eq!(original_records, converted_records);
}

#[cfg(feature = "polars")]
#[test]
fn test_large_dataset() {
    let large_records: Vec<BasicRecord> = (0..1000)
        .map(|i| BasicRecord {
            id: i,
            name: format!("User_{}", i),
            score: (i as f64) * 0.01,
            active: i % 2 == 0,
        })
        .collect();

    let df = to_dataframe(&large_records).expect("Failed to convert large dataset");
    assert_eq!(df.height(), 1000);

    let converted: Vec<BasicRecord> =
        from_dataframe(df).expect("Failed to convert large dataset back");
    assert_eq!(large_records, converted);
}
