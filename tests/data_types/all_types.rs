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
struct AllTypesRecord {
    // Integer types
    int8_val: i8,
    int16_val: i16,
    int32_val: i32,
    int64_val: i64,
    uint8_val: u8,
    uint16_val: u16,
    uint32_val: u32,
    uint64_val: u64,
    
    // Float types
    float32_val: f32,
    float64_val: f64,
    
    // String and char
    string_val: String,
    
    // Boolean
    bool_val: bool,
}


#[test]
fn test_all_numeric_types() {
    let records = vec![
        AllTypesRecord {
            int8_val: -128,
            int16_val: -32768,
            int32_val: -2147483648,
            int64_val: -9223372036854775808,
            uint8_val: 255,
            uint16_val: 65535,
            uint32_val: 4294967295,
            uint64_val: 18446744073709551615,
            float32_val: 3.14159,
            float64_val: 2.718281828459045,
            string_val: "Hello, World!".to_string(),
            bool_val: true,
        },
        AllTypesRecord {
            int8_val: 127,
            int16_val: 32767,
            int32_val: 2147483647,
            int64_val: 9223372036854775807,
            uint8_val: 0,
            uint16_val: 0,
            uint32_val: 0,
            uint64_val: 0,
            float32_val: -3.14159,
            float64_val: -2.718281828459045,
            string_val: "Goodbye, World!".to_string(),
            bool_val: false,
        },
    ];

    let df = to_dataframe(&records).expect("Failed to convert all types");
    let converted: Vec<AllTypesRecord> = from_dataframe(df).expect("Failed to convert back");
    
    assert_eq!(records, converted);
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct SpecialFloats {
    normal: f64,
    zero: f64,
    negative_zero: f64,
    infinity: f64,
    neg_infinity: f64,
    // Note: NaN doesn't equal itself, so we'll test it separately
}

#[test]
fn test_special_float_values() {
    let records = vec![
        SpecialFloats {
            normal: 42.0,
            zero: 0.0,
            negative_zero: -0.0,
            infinity: f64::INFINITY,
            neg_infinity: f64::NEG_INFINITY,
        },
    ];

    let df = to_dataframe(&records).expect("Failed to convert special floats");
    let converted: Vec<SpecialFloats> = from_dataframe(df).expect("Failed to convert back");
    
    assert_eq!(records, converted);
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct StringVariations {
    empty: String,
    ascii: String,
    unicode: String,
    emoji: String,
    whitespace: String,
    newlines: String,
}

#[test]
fn test_string_variations() {
    let records = vec![
        StringVariations {
            empty: "".to_string(),
            ascii: "Hello World".to_string(),
            unicode: "こんにちは世界".to_string(),
            emoji: "🦀🚀✨".to_string(),
            whitespace: "   spaces   ".to_string(),
            newlines: "line1\nline2\r\nline3".to_string(),
        },
        StringVariations {
            empty: "".to_string(),
            ascii: "Another test".to_string(),
            unicode: "Здравствуй мир".to_string(),
            emoji: "🎉🎊🎈".to_string(),
            whitespace: "\t\ttabs\t\t".to_string(),
            newlines: "single\nline".to_string(),
        },
    ];

    let df = to_dataframe(&records).expect("Failed to convert strings");
    let converted: Vec<StringVariations> = from_dataframe(df).expect("Failed to convert back");
    
    assert_eq!(records, converted);
}
