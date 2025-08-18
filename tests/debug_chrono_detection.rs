use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use serde_polars::to_dataframe;

// Reproduce the user's exact issue
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TestRecord {
    id: i64,
    calc_min_date: i32,  // This is causing the issue - integer field being treated as chrono
    name: String,
    birth_date: NaiveDate,  // This is a legitimate chrono field
}

#[test]
fn test_debug_chrono_detection() {
    // Enable detection debugging by manually calling the internal functions
    use serde_polars::detect_chrono_types;
    
    let record = TestRecord {
        id: 1,
        calc_min_date: 20240101,
        name: "Test".to_string(),
        birth_date: NaiveDate::from_ymd_opt(1990, 5, 15).unwrap(),
    };
    
    // Debug: Check what chrono types are detected
    let detected_types = detect_chrono_types(&record).unwrap();
    println!("Detected chrono types: {:?}", detected_types);
    
    // This should only show "birth_date" -> "NaiveDate", NOT "calc_min_date"
    assert!(!detected_types.contains_key("calc_min_date"), 
            "calc_min_date should NOT be detected as a chrono type!");
    
    // Now try the actual conversion
    let records = vec![record];
    println!("Attempting to_dataframe conversion...");
    
    match to_dataframe(&records) {
        Ok(df) => {
            println!("✓ Successfully created DataFrame with {} rows", df.height());
            
            // Debug: Print the DataFrame schema
            println!("DataFrame schema:");
            for (name, dtype) in df.schema().iter() {
                println!("  {}: {:?}", name, dtype);
            }
            
            // Now test the reverse conversion - this is where the error likely occurs
            println!("\nAttempting from_dataframe conversion...");
            use serde_polars::from_dataframe;
            
            match from_dataframe::<TestRecord>(df) {
                Ok(converted_back) => {
                    println!("✓ Successfully converted back {} records", converted_back.len());
                    println!("Converted records: {:?}", converted_back);
                },
                Err(e) => {
                    println!("✗ Error in from_dataframe: {}", e);
                    panic!("Reverse conversion failed: {}", e);
                }
            }
        },
        Err(e) => {
            println!("✗ Error in to_dataframe: {}", e);
            panic!("Conversion failed: {}", e);
        }
    }
}