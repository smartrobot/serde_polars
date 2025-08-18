use chrono::{NaiveDate, NaiveDateTime, DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_polars::{to_dataframe};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct OptionalChronoRecord {
    id: i64,
    name: String,
    birth_date: Option<NaiveDate>,
    last_login: Option<NaiveDateTime>,
    deleted_at: Option<DateTime<Utc>>,
}

#[test]
fn test_debug_array_types() {
    use serde_arrow::{to_record_batch, schema::{TracingOptions, SchemaLike}};
    use std::sync::Arc;
    
    let single_record = vec![
        OptionalChronoRecord {
            id: 42,
            name: "Only One".to_string(),
            birth_date: Some(NaiveDate::from_ymd_opt(1995, 7, 20).unwrap()),
            last_login: Some(NaiveDate::from_ymd_opt(2023, 6, 15).unwrap().and_hms_opt(14, 30, 0).unwrap()),
            deleted_at: None,  // This field is None
        },
    ];
    
    // Create the basic record batch to see what types are created
    let tracing_options = TracingOptions::default()
        .enums_without_data_as_strings(true)
        .allow_null_fields(true)
        .map_as_struct(false)
        .string_dictionary_encoding(false)
        .coerce_numbers(false);
    
    let fields: Vec<Arc<arrow::datatypes::Field>> = Vec::from_samples(&single_record, tracing_options).unwrap();
    
    match to_record_batch(&fields, &single_record) {
        Ok(rb) => {
            println!("Successfully created RecordBatch with {} columns:", rb.num_columns());
            let schema = rb.schema();
            for (i, column) in rb.columns().iter().enumerate() {
                let field = schema.field(i);
                println!("  Column '{}': type = {:?}, array_type = {}", 
                         field.name(), 
                         field.data_type(),
                         column.data_type());
            }
        },
        Err(e) => {
            println!("Error creating RecordBatch: {}", e);
        }
    }
}