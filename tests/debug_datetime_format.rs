use chrono::{NaiveDate, NaiveDateTime, DateTime, Utc};
use serde_json;

#[test] 
fn debug_datetime_serialization() {
    let naive_date = NaiveDate::from_ymd_opt(2023, 6, 15).unwrap();
    let naive_datetime = NaiveDate::from_ymd_opt(2023, 6, 15).unwrap().and_hms_opt(14, 30, 0).unwrap();
    let datetime_utc = DateTime::parse_from_rfc3339("2023-06-15T14:30:00Z").unwrap().with_timezone(&Utc);
    
    println!("NaiveDate serializes to: {}", serde_json::to_string(&naive_date).unwrap());
    println!("NaiveDateTime serializes to: {}", serde_json::to_string(&naive_datetime).unwrap());
    println!("DateTime<Utc> serializes to: {}", serde_json::to_string(&datetime_utc).unwrap());
}