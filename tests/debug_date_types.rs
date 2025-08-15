use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use serde_polars::to_dataframe;

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
struct DateRecord {
    id: i32,
    date_field: NaiveDate,
    name: String,
}

#[test]
fn debug_date_column_type() {
    let records = vec![
        DateRecord {
            id: 1,
            date_field: NaiveDate::from_ymd_opt(2023, 6, 15).unwrap(),
            name: "Test".to_string(),
        },
    ];

    let df = to_dataframe(&records).unwrap();
    
    // Print the schema to see what types we actually get
    println!("DataFrame Schema:");
    for (name, dtype) in df.schema().iter() {
        println!("  {}: {:?}", name, dtype);
    }
    
    // Print the actual data
    println!("\nDataFrame:");
    println!("{}", df);
    
    // Check specific column type
    let date_column = df.column("date_field").unwrap();
    println!("\nDate column type: {:?}", date_column.dtype());
    
    // This should be Date type, not String!
    assert_ne!(date_column.dtype(), &DataType::String);
}