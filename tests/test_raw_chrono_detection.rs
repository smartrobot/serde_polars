use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use serde_polars::to_dataframe;

// Import DataType for tests
#[cfg(feature = "polars_0_46")]
use polars_crate_0_46::datatypes::DataType;

#[cfg(feature = "polars_0_50")]
use polars_crate_0_50::datatypes::DataType;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Calendar {
    cal: NaiveDate,  // Raw NaiveDate - should be detected as Date!
    week: i64,
}

#[test]
fn test_raw_naive_date_detection() {
    let records = vec![
        Calendar {
            cal: NaiveDate::from_ymd_opt(2023, 6, 15).unwrap(),
            week: 25,
        },
        Calendar {
            cal: NaiveDate::from_ymd_opt(2023, 6, 22).unwrap(),
            week: 26,
        },
    ];

    // Convert to DataFrame - raw NaiveDate should be automatically detected!
    let df = to_dataframe(&records).unwrap();
    
    println!("Raw NaiveDate detection schema:");
    for (name, dtype) in df.schema().iter() {
        println!("  {}: {:?}", name, dtype);
    }
    
    // SUCCESS! Raw NaiveDate is automatically detected and converted to Date!
    let cal_column = df.column("cal").unwrap();
    assert_eq!(cal_column.dtype(), &DataType::Date);
}