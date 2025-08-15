use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use serde_polars::DateWrapper;
use serde_arrow::schema::{SchemaLike, TracingOptions};
use arrow::datatypes::FieldRef;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TestRecord {
    id: i32,
    birth_date: DateWrapper,
    name: String,
}

#[test]
fn debug_schema_creation() {
    let data = vec![TestRecord {
        id: 1,
        birth_date: DateWrapper(NaiveDate::from_ymd_opt(1990, 5, 15).unwrap()),
        name: "Alice".to_string(),
    }];
    
    let tracing_options = TracingOptions::default();
    
    // Test from_type
    println!("Testing from_type:");
    let from_type_result = Vec::<FieldRef>::from_type::<TestRecord>(tracing_options.clone());
    match from_type_result {
        Ok(fields) => {
            println!("from_type succeeded:");
            for field in &fields {
                println!("  {}: {:?}", field.name(), field.data_type());
            }
        },
        Err(e) => println!("from_type failed: {:?}", e),
    }
    
    // Test from_samples
    println!("\nTesting from_samples:");
    let from_samples_result = Vec::<FieldRef>::from_samples(&data, tracing_options);
    match from_samples_result {
        Ok(fields) => {
            println!("from_samples succeeded:");
            for field in &fields {
                println!("  {}: {:?}", field.name(), field.data_type());
            }
        },
        Err(e) => println!("from_samples failed: {:?}", e),
    }
}