use serde::{Deserialize, Serialize};
use serde_polars::{from_dataframe, to_dataframe};

#[cfg(feature = "polars")]
use polars::prelude::*;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct VersionTestRecord {
    id: i64,
    name: String,
    value: f64,
}

#[cfg(all(feature = "polars", feature = "polars-0-40"))]
#[test]
fn test_polars_0_40_compatibility() {
    let records = vec![
        VersionTestRecord {
            id: 1,
            name: "Polars 0.40".to_string(),
            value: 40.0,
        },
    ];
    
    let df = to_dataframe(&records).expect("Failed with polars-0-40");
    let converted: Vec<VersionTestRecord> = from_dataframe(df).expect("Failed to convert back");
    assert_eq!(records, converted);
}

#[cfg(all(feature = "polars", feature = "polars-0-41"))]
#[test]
fn test_polars_0_41_compatibility() {
    let records = vec![
        VersionTestRecord {
            id: 1,
            name: "Polars 0.41".to_string(),
            value: 41.0,
        },
    ];
    
    let df = to_dataframe(&records).expect("Failed with polars-0-41");
    let converted: Vec<VersionTestRecord> = from_dataframe(df).expect("Failed to convert back");
    assert_eq!(records, converted);
}

#[cfg(all(feature = "polars", feature = "polars-0-42"))]
#[test]
fn test_polars_0_42_compatibility() {
    let records = vec![
        VersionTestRecord {
            id: 1,
            name: "Polars 0.42".to_string(),
            value: 42.0,
        },
    ];
    
    let df = to_dataframe(&records).expect("Failed with polars-0-42");
    let converted: Vec<VersionTestRecord> = from_dataframe(df).expect("Failed to convert back");
    assert_eq!(records, converted);
}

#[cfg(all(feature = "polars", feature = "polars-0-43"))]
#[test]
fn test_polars_0_43_compatibility() {
    let records = vec![
        VersionTestRecord {
            id: 1,
            name: "Polars 0.43".to_string(),
            value: 43.0,
        },
    ];
    
    let df = to_dataframe(&records).expect("Failed with polars-0-43");
    let converted: Vec<VersionTestRecord> = from_dataframe(df).expect("Failed to convert back");
    assert_eq!(records, converted);
}

#[cfg(all(feature = "polars", feature = "polars-0-44"))]
#[test]
fn test_polars_0_44_compatibility() {
    let records = vec![
        VersionTestRecord {
            id: 1,
            name: "Polars 0.44".to_string(),
            value: 44.0,
        },
    ];
    
    let df = to_dataframe(&records).expect("Failed with polars-0-44");
    let converted: Vec<VersionTestRecord> = from_dataframe(df).expect("Failed to convert back");
    assert_eq!(records, converted);
}

#[cfg(all(feature = "polars", feature = "polars-0-45"))]
#[test]
fn test_polars_0_45_compatibility() {
    let records = vec![
        VersionTestRecord {
            id: 1,
            name: "Polars 0.45".to_string(),
            value: 45.0,
        },
    ];
    
    let df = to_dataframe(&records).expect("Failed with polars-0-45");
    let converted: Vec<VersionTestRecord> = from_dataframe(df).expect("Failed to convert back");
    assert_eq!(records, converted);
}

#[cfg(all(feature = "polars", feature = "polars-0-46"))]
#[test]
fn test_polars_0_46_compatibility() {
    let records = vec![
        VersionTestRecord {
            id: 1,
            name: "Polars 0.46".to_string(),
            value: 46.0,
        },
    ];
    
    let df = to_dataframe(&records).expect("Failed with polars-0-46");
    let converted: Vec<VersionTestRecord> = from_dataframe(df).expect("Failed to convert back");
    assert_eq!(records, converted);
}

#[cfg(all(feature = "polars", feature = "polars-0-47"))]
#[test]
fn test_polars_0_47_compatibility() {
    let records = vec![
        VersionTestRecord {
            id: 1,
            name: "Polars 0.47".to_string(),
            value: 47.0,
        },
    ];
    
    let df = to_dataframe(&records).expect("Failed with polars-0-47");
    let converted: Vec<VersionTestRecord> = from_dataframe(df).expect("Failed to convert back");
    assert_eq!(records, converted);
}

#[cfg(all(feature = "polars", feature = "polars-0-48"))]
#[test]
fn test_polars_0_48_compatibility() {
    let records = vec![
        VersionTestRecord {
            id: 1,
            name: "Polars 0.48".to_string(),
            value: 48.0,
        },
    ];
    
    let df = to_dataframe(&records).expect("Failed with polars-0-48");
    let converted: Vec<VersionTestRecord> = from_dataframe(df).expect("Failed to convert back");
    assert_eq!(records, converted);
}

#[cfg(all(feature = "polars", feature = "polars-0-49"))]
#[test]
fn test_polars_0_49_compatibility() {
    let records = vec![
        VersionTestRecord {
            id: 1,
            name: "Polars 0.49".to_string(),
            value: 49.0,
        },
    ];
    
    let df = to_dataframe(&records).expect("Failed with polars-0-49");
    let converted: Vec<VersionTestRecord> = from_dataframe(df).expect("Failed to convert back");
    assert_eq!(records, converted);
}

#[cfg(all(feature = "polars", feature = "polars-0-50"))]
#[test]
fn test_polars_0_50_compatibility() {
    let records = vec![
        VersionTestRecord {
            id: 1,
            name: "Polars 0.50".to_string(),
            value: 50.0,
        },
    ];
    
    let df = to_dataframe(&records).expect("Failed with polars-0-50");
    let converted: Vec<VersionTestRecord> = from_dataframe(df).expect("Failed to convert back");
    assert_eq!(records, converted);
}

// Test that prints which version is actually being used
#[cfg(feature = "polars")]
#[test]
fn test_print_active_version() {
    println!("Testing with Polars version features:");
    
    #[cfg(feature = "polars-0-40")]
    println!("  ✓ polars-0-40 is enabled");
    #[cfg(feature = "polars-0-41")]
    println!("  ✓ polars-0-41 is enabled");
    #[cfg(feature = "polars-0-42")]
    println!("  ✓ polars-0-42 is enabled");
    #[cfg(feature = "polars-0-43")]
    println!("  ✓ polars-0-43 is enabled");
    #[cfg(feature = "polars-0-44")]
    println!("  ✓ polars-0-44 is enabled");
    #[cfg(feature = "polars-0-45")]
    println!("  ✓ polars-0-45 is enabled");
    #[cfg(feature = "polars-0-46")]
    println!("  ✓ polars-0-46 is enabled");
    #[cfg(feature = "polars-0-47")]
    println!("  ✓ polars-0-47 is enabled");
    #[cfg(feature = "polars-0-48")]
    println!("  ✓ polars-0-48 is enabled");
    #[cfg(feature = "polars-0-49")]
    println!("  ✓ polars-0-49 is enabled");
    #[cfg(feature = "polars-0-50")]
    println!("  ✓ polars-0-50 is enabled");
}