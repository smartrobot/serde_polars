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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct NestedRecord {
    id: i64,
    metadata: MetadataRecord,
    scores: Vec<f64>,
    tags: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct MetadataRecord {
    created_by: String,
    version: i32,
    is_active: bool,
}

// Note: This test may not work as expected since Polars doesn't natively support
// nested structures in the same way. We'll test what's possible.
#[test]
fn test_flattened_nested_structures() {
    // For nested structures, we typically need to flatten them
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct FlattenedRecord {
        id: i64,
        created_by: String,
        version: i32,
        is_active: bool,
        score_1: f64,
        score_2: f64,
        tag_1: String,
        tag_2: String,
    }

    let records = vec![
        FlattenedRecord {
            id: 1,
            created_by: "Alice".to_string(),
            version: 1,
            is_active: true,
            score_1: 85.5,
            score_2: 90.0,
            tag_1: "important".to_string(),
            tag_2: "verified".to_string(),
        },
        FlattenedRecord {
            id: 2,
            created_by: "Bob".to_string(),
            version: 2,
            is_active: false,
            score_1: 78.3,
            score_2: 82.1,
            tag_1: "draft".to_string(),
            tag_2: "pending".to_string(),
        },
    ];

    let df = to_dataframe(&records).expect("Failed to convert flattened structures");
    let converted: Vec<FlattenedRecord> = from_dataframe(df).expect("Failed to convert back");

    assert_eq!(records, converted);
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct EnumRecord {
    id: i64,
    status: Status,
    priority: Priority,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum Status {
    Pending,
    InProgress,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum Priority {
    Low,
    Medium,
    High,
    Critical,
}

// NOTE: Direct enum serialization has limitations with serde_arrow/polars interchange
// For working enum support, see tests/enum_working.rs which shows the proper pattern

#[test]
#[ignore = "Direct enum serialization requires categorical feature - see enum_working.rs for working pattern"]
fn test_enum_serialization() {
    let records = vec![
        EnumRecord {
            id: 1,
            status: Status::Pending,
            priority: Priority::High,
        },
        EnumRecord {
            id: 2,
            status: Status::InProgress,
            priority: Priority::Medium,
        },
        EnumRecord {
            id: 3,
            status: Status::Completed,
            priority: Priority::Low,
        },
        EnumRecord {
            id: 4,
            status: Status::Failed,
            priority: Priority::Critical,
        },
    ];

    let df = to_dataframe(&records).expect("Failed to convert enum records");

    // Verify the DataFrame structure - enums should be string columns
    println!("DataFrame schema:");
    println!("{:?}", df.schema());
    println!("DataFrame content:");
    println!("{}", df);

    let converted: Vec<EnumRecord> = from_dataframe(df).expect("Failed to convert back");

    assert_eq!(records, converted);
    println!("✅ Enum serialization test passed! Enums are stored as strings in Polars.");
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TupleRecord {
    id: i64,
    coordinates: (f64, f64),
    rgb: (u8, u8, u8),
}

// NOTE: Tuple serialization is currently not supported by serde_arrow/polars
// This test shows the limitation and is commented out

#[test]
#[ignore = "Tuples not supported by serde_arrow/polars interchange"]
fn test_tuple_serialization() {
    let records = vec![
        TupleRecord {
            id: 1,
            coordinates: (40.7128, -74.0060), // NYC
            rgb: (255, 0, 0),                 // Red
        },
        TupleRecord {
            id: 2,
            coordinates: (34.0522, -118.2437), // LA
            rgb: (0, 255, 0),                  // Green
        },
    ];

    let df = to_dataframe(&records).expect("Failed to convert tuple records");
    let converted: Vec<TupleRecord> = from_dataframe(df).expect("Failed to convert back");

    assert_eq!(records, converted);
}

// Test with newtype patterns
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct UserId(i64);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Email(String);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct NewtypeRecord {
    user_id: UserId,
    email: Email,
    score: f64,
}

#[test]
fn test_newtype_wrappers() {
    let records = vec![
        NewtypeRecord {
            user_id: UserId(1001),
            email: Email("alice@example.com".to_string()),
            score: 95.5,
        },
        NewtypeRecord {
            user_id: UserId(1002),
            email: Email("bob@example.com".to_string()),
            score: 87.3,
        },
    ];

    let df = to_dataframe(&records).expect("Failed to convert newtype records");
    let converted: Vec<NewtypeRecord> = from_dataframe(df).expect("Failed to convert back");

    assert_eq!(records, converted);
}

// ============================================================================
// WORKING ENUM PATTERN - The recommended approach for enum support
// ============================================================================

// This is the enum we want to use in our application
#[derive(Debug, Clone, PartialEq)]
pub enum WorkingStatus {
    Active,
    Inactive,
    Pending,
    Failed,
}

// This is the struct that works with Polars - using strings instead of enums
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct PolarsCompatibleRecord {
    id: i64,
    name: String,
    status: String, // Store enum as string for Polars compatibility
    score: f64,
}

// This is your application struct with the actual enum
#[derive(Debug, Clone, PartialEq)]
struct ApplicationRecord {
    id: i64,
    name: String,
    status: WorkingStatus, // Real enum
    score: f64,
}

impl From<ApplicationRecord> for PolarsCompatibleRecord {
    fn from(app: ApplicationRecord) -> Self {
        PolarsCompatibleRecord {
            id: app.id,
            name: app.name,
            status: match app.status {
                WorkingStatus::Active => "Active".to_string(),
                WorkingStatus::Inactive => "Inactive".to_string(),
                WorkingStatus::Pending => "Pending".to_string(),
                WorkingStatus::Failed => "Failed".to_string(),
            },
            score: app.score,
        }
    }
}

impl From<PolarsCompatibleRecord> for ApplicationRecord {
    fn from(polars: PolarsCompatibleRecord) -> Self {
        ApplicationRecord {
            id: polars.id,
            name: polars.name,
            status: match polars.status.as_str() {
                "Active" => WorkingStatus::Active,
                "Inactive" => WorkingStatus::Inactive,
                "Pending" => WorkingStatus::Pending,
                "Failed" => WorkingStatus::Failed,
                _ => WorkingStatus::Failed, // Default fallback
            },
            score: polars.score,
        }
    }
}

// Convenience functions for working with enums
fn app_records_to_dataframe(
    records: &[ApplicationRecord],
) -> serde_polars::Result<polars::prelude::DataFrame> {
    let polars_records: Vec<PolarsCompatibleRecord> =
        records.iter().map(|r| r.clone().into()).collect();
    to_dataframe(&polars_records)
}

fn dataframe_to_app_records(
    df: polars::prelude::DataFrame,
) -> serde_polars::Result<Vec<ApplicationRecord>> {
    let polars_records: Vec<PolarsCompatibleRecord> = from_dataframe(df)?;
    Ok(polars_records.into_iter().map(|r| r.into()).collect())
}


#[test]
fn test_working_enum_pattern() {
    // Application records with enums
    let app_records = vec![
        ApplicationRecord {
            id: 1,
            name: "Alice".to_string(),
            status: WorkingStatus::Active,
            score: 95.5,
        },
        ApplicationRecord {
            id: 2,
            name: "Bob".to_string(),
            status: WorkingStatus::Inactive,
            score: 87.2,
        },
        ApplicationRecord {
            id: 3,
            name: "Charlie".to_string(),
            status: WorkingStatus::Pending,
            score: 92.8,
        },
        ApplicationRecord {
            id: 4,
            name: "Diana".to_string(),
            status: WorkingStatus::Failed,
            score: 76.3,
        },
    ];

    // Convert to DataFrame using our helper function
    let df = app_records_to_dataframe(&app_records).expect("Failed to convert to DataFrame");

    // Verify the DataFrame structure - enums are stored as strings
    assert_eq!(df.width(), 4);
    assert_eq!(df.height(), 4);

    // Convert back to application records
    let converted_records = dataframe_to_app_records(df).expect("Failed to convert back");

    // Verify roundtrip accuracy
    assert_eq!(app_records, converted_records);

    println!("✅ Working enum pattern test passed!");
}


#[test]
fn test_enum_with_polars_created_values() {
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

    // Create a DataFrame directly in Polars with enum values as strings
    let df = df![
        "id" => [100i64, 200i64, 300i64, 400i64],
        "name" => ["Alice", "Bob", "Charlie", "Diana"],
        "status" => ["Active", "Pending", "Inactive", "Failed"],  // Enum values created in Polars
        "score" => [95.5f64, 87.2f64, 92.8f64, 76.3f64]
    ]
    .expect("Failed to create DataFrame");

    // Convert this Polars-created DataFrame to our application records
    let app_records =
        dataframe_to_app_records(df).expect("Failed to convert Polars-created DataFrame");

    // Verify the enum values were correctly parsed
    assert_eq!(app_records[0].status, WorkingStatus::Active);
    assert_eq!(app_records[1].status, WorkingStatus::Pending);
    assert_eq!(app_records[2].status, WorkingStatus::Inactive);
    assert_eq!(app_records[3].status, WorkingStatus::Failed); // All created directly in Polars!

    println!("✅ Enum deserialization works with Polars-created values!");
}
