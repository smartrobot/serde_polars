# serde_polars

[![Crates.io](https://img.shields.io/crates/v/serde_polars)](https://crates.io/crates/serde_polars)
[![Documentation](https://docs.rs/serde_polars/badge.svg)](https://docs.rs/serde_polars)
[![License](https://img.shields.io/crates/l/serde_polars)](https://github.com/your-username/serde_polars)

**High-performance serde integration for Polars DataFrames**

Seamlessly convert between Polars DataFrames and Rust structs with serde support.

## 🚀 Features

- **🔄 Bidirectional Conversion**: `Vec<T> ↔ DataFrame` where `T` implements `Serialize`/`Deserialize`
- **🎯 Multi-Version Support**: Compatible with Polars 0.40 through 0.50 via feature flags
- **🧵 Thread-Safe**: Safe for concurrent use across multiple threads
- **⚡ High Performance**: Minimal allocations with efficient Arrow-based conversion
- **🏷️ Enum Support**: Clean enum handling via string conversion pattern
- **🔧 Type Safety**: Strong typing with comprehensive error handling
- **📊 Production Ready**: Comprehensive test suite including concurrent usage scenarios

## 📦 Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
# Include the Polars version you're using
polars = "0.46"
serde = { version = "1.0", features = ["derive"] }

# Match the feature to your Polars version
serde_polars = { version = "0.1", features = ["polars-0-46"] }
```

## 🎯 Quick Start

```rust
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use serde_polars::{from_dataframe, to_dataframe};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Person {
    name: String,
    age: i32,
    salary: f64,
    active: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Your application data
    let people = vec![
        Person { name: "Alice".to_string(), age: 30, salary: 75000.0, active: true },
        Person { name: "Bob".to_string(), age: 25, salary: 60000.0, active: false },
    ];

    // Convert to DataFrame for processing
    let mut df = to_dataframe(&people)?;
    
    // Process with Polars - filter, aggregate, transform, etc.
    df = df.filter(col("salary").gt(lit(65000)))?;
    
    // Convert back to structs
    let filtered_people: Vec<Person> = from_dataframe(df)?;
    
    println!("{:#?}", filtered_people);
    Ok(())
}
```

## 🏷️ Enum Support

Enums are supported through a clean string conversion pattern:

```rust
use serde::{Deserialize, Serialize};
use serde_polars::{from_dataframe, to_dataframe};

// Your application enum
#[derive(Debug, Clone, PartialEq)]
enum Status { Active, Inactive, Pending, Failed }

// Polars-compatible struct (enums as strings)
#[derive(Serialize, Deserialize)]
struct Record {
    id: i64,
    name: String,
    status: String,  // Enum stored as string
    score: f64,
}

// Your application struct (with real enums)
struct AppRecord {
    id: i64,
    name: String,
    status: Status,  // Real enum
    score: f64,
}

// Implement conversions
impl From<AppRecord> for Record {
    fn from(app: AppRecord) -> Self {
        Record {
            id: app.id,
            name: app.name,
            status: match app.status {
                Status::Active => "Active".to_string(),
                Status::Inactive => "Inactive".to_string(),
                Status::Pending => "Pending".to_string(),
                Status::Failed => "Failed".to_string(),
            },
            score: app.score,
        }
    }
}

impl From<Record> for AppRecord {
    fn from(record: Record) -> Self {
        AppRecord {
            id: record.id,
            name: record.name,
            status: match record.status.as_str() {
                "Active" => Status::Active,
                "Inactive" => Status::Inactive,
                "Pending" => Status::Pending,
                _ => Status::Failed,
            },
            score: record.score,
        }
    }
}
```

**Benefits of this pattern:**
- ✅ Strong typing in your application code
- ✅ Full Polars compatibility (filtering, grouping, etc.)
- ✅ Works with Polars-created enum values (`lit("Active")`)
- ✅ Thread-safe and performant

## 🧵 Thread Safety

Safe for concurrent use across multiple threads:

```rust
use std::thread;
use serde_polars::{from_dataframe, to_dataframe};

// Process data concurrently across threads
let handles: Vec<_> = datasets
    .into_iter()
    .map(|data| {
        thread::spawn(move || {
            // Convert to DataFrame
            let df = to_dataframe(&data)?;
            
            // Process with Polars
            let processed = df
                .filter(col("score").gt(lit(80.0)))?
                .group_by([col("category")])?
                .agg([col("value").mean()])?;
                
            // Convert back
            from_dataframe(processed)
        })
    })
    .collect();

// Collect results
let results: Vec<_> = handles
    .into_iter()
    .map(|h| h.join().unwrap())
    .collect();
```

## 🎛️ Version Compatibility

| Polars Version | Feature Flag | Status |
|----------------|--------------|---------|
| 0.40.x | `polars-0-40` | ✅ Supported |
| 0.41.x | `polars-0-41` | ✅ Supported |
| 0.42.x | `polars-0-42` | ✅ Supported |
| 0.43.x | `polars-0-43` | ✅ Supported |
| 0.44.x | `polars-0-44` | ✅ Supported |
| 0.45.x | `polars-0-45` | ✅ Supported |
| 0.46.x | `polars-0-46` | ✅ Supported |
| 0.47.x | `polars-0-47` | ✅ Supported |
| 0.48.x | `polars-0-48` | ✅ Supported |
| 0.49.x | `polars-0-49` | ✅ Supported |
| 0.50.x | `polars-0-50` | ✅ Supported |

**Important**: 
- The Polars version and feature flag must match exactly!
- Version features are **mutually exclusive** - only enable one at a time
- Never use `--all-features` as it will cause compilation errors

```toml
# Example for Polars 0.44
[dependencies]
polars = "0.44"
serde_polars = { version = "0.1", default-features = false, features = ["polars-0-44"] }
```

## 📊 Supported Data Types

| Rust Type | Polars Type | Status |
|-----------|-------------|---------|
| `i8`, `i16`, `i32`, `i64` | `Int8`, `Int16`, `Int32`, `Int64` | ✅ |
| `u8`, `u16`, `u32`, `u64` | `UInt8`, `UInt16`, `UInt32`, `UInt64` | ✅ |
| `f32`, `f64` | `Float32`, `Float64` | ✅ |
| `bool` | `Boolean` | ✅ |
| `String` | `String` | ✅ |
| `Option<T>` | `Nullable<T>` | ✅ |
| Enums (via strings) | `String` | ✅ |
| Nested structs (flattened) | Multiple columns | ✅ |
| Newtype wrappers | Underlying type | ✅ |

## 🔧 Error Handling

```rust
use serde_polars::{from_dataframe, to_dataframe, PolarsSerdeError};

match to_dataframe(&data) {
    Ok(df) => { /* success */ },
    Err(PolarsSerdeError::EmptyInput) => {
        println!("Cannot convert empty vector");
    },
    Err(PolarsSerdeError::SerdeArrowError(e)) => {
        println!("Serialization error: {}", e);
    },
    Err(PolarsSerdeError::InterchangeError(e)) => {
        println!("Polars conversion error: {}", e);
    },
    Err(PolarsSerdeError::ConversionError { message }) => {
        println!("Conversion error: {}", message);
    },
}
```

## 🧪 Testing

Run the comprehensive test suite:

```bash
# Run all tests (uses default polars-0-40 feature)
cargo test

# Run specific test categories
cargo test multithreading
cargo test complex_structures

# Test with a different Polars version
cargo test --no-default-features --features polars-0-46

# IMPORTANT: Never use --all-features as version features are mutually exclusive
```

## 📈 Performance

- **Efficient Arrow-based conversion** using columnar data format
- **Minimal allocations** with pre-sized vectors  
- **Thread-safe** for concurrent processing
- **Efficient enum conversion** through string mapping

## 🤝 Contributing

Contributions are welcome! Please ensure:

1. All tests pass: `cargo test --all-features`
2. Code is formatted: `cargo fmt`
3. No clippy warnings: `cargo clippy`
4. Add tests for new functionality

## 📄 License

Licensed under the MIT License. See [LICENSE](LICENSE) for details.

## 🔗 Related Projects

- [Polars](https://github.com/pola-rs/polars) - Fast multi-threaded DataFrame library
- [serde](https://github.com/serde-rs/serde) - Serialization framework for Rust
- [Arrow](https://github.com/apache/arrow-rs) - Columnar in-memory analytics

---

**Happy data processing!** 🚀📊✨