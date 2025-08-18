use chrono::{NaiveDate, NaiveDateTime, DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_polars::{from_dataframe, to_dataframe};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct OptionalChronoRecord {
    id: i64,
    name: String,
    birth_date: Option<NaiveDate>,
    last_login: Option<NaiveDateTime>,
    deleted_at: Option<DateTime<Utc>>,
}

#[test]
fn test_debug_single_record() {
    let single_record = vec![
        OptionalChronoRecord {
            id: 42,
            name: "Only One".to_string(),
            birth_date: Some(NaiveDate::from_ymd_opt(1995, 7, 20).unwrap()),
            last_login: Some(NaiveDate::from_ymd_opt(2023, 6, 15).unwrap().and_hms_opt(14, 30, 0).unwrap()),
            deleted_at: None,
        },
    ];
    
    println!("Testing to_dataframe with single record...");
    match to_dataframe(&single_record) {
        Ok(df) => {
            println!("✓ Successfully created DataFrame with {} rows", df.height());
            
            println!("Testing from_dataframe...");
            match from_dataframe::<OptionalChronoRecord>(df) {
                Ok(converted_back) => {
                    println!("✓ Successfully converted back {} records", converted_back.len());
                },
                Err(e) => {
                    println!("✗ Error in from_dataframe: {}", e);
                }
            }
        },
        Err(e) => {
            println!("✗ Error in to_dataframe: {}", e);
        }
    }
}