use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use serde::{Deserialize, Serialize};
use serde_polars::{from_dataframe, to_dataframe};

#[cfg(feature = "polars")]
use polars::prelude::*;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct BenchRecord {
    id: i64,
    name: String,
    score: f64,
    active: bool,
    timestamp: i64,
}

impl BenchRecord {
    fn generate(id: i64) -> Self {
        Self {
            id,
            name: format!("User_{}", id),
            score: (id as f64) * 0.1,
            active: id % 2 == 0,
            timestamp: 1609459200 + id, // Base timestamp + offset
        }
    }
}

#[cfg(feature = "polars")]
fn bench_to_dataframe(c: &mut Criterion) {
    let mut group = c.benchmark_group("to_dataframe");

    for size in [100, 1_000, 10_000, 100_000].iter() {
        let records: Vec<BenchRecord> = (0..*size).map(BenchRecord::generate).collect();

        group.bench_with_input(BenchmarkId::new("records", size), size, |b, &_size| {
            b.iter(|| {
                let _df = to_dataframe(&records).expect("Failed to convert to DataFrame");
            });
        });
    }

    group.finish();
}

#[cfg(feature = "polars")]
fn bench_from_dataframe(c: &mut Criterion) {
    let mut group = c.benchmark_group("from_dataframe");

    for size in [100, 1_000, 10_000, 100_000].iter() {
        let records: Vec<BenchRecord> = (0..*size).map(BenchRecord::generate).collect();
        let df = to_dataframe(&records).expect("Failed to create test DataFrame");

        group.bench_with_input(BenchmarkId::new("records", size), size, |b, &_size| {
            b.iter(|| {
                let _records: Vec<BenchRecord> =
                    from_dataframe(df.clone()).expect("Failed to convert from DataFrame");
            });
        });
    }

    group.finish();
}

#[cfg(feature = "polars")]
fn bench_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("roundtrip");

    for size in [100, 1_000, 10_000].iter() {
        let records: Vec<BenchRecord> = (0..*size).map(BenchRecord::generate).collect();

        group.bench_with_input(BenchmarkId::new("records", size), size, |b, &_size| {
            b.iter(|| {
                let df = to_dataframe(&records).expect("Failed to convert to DataFrame");
                let _converted: Vec<BenchRecord> =
                    from_dataframe(df).expect("Failed to convert back");
            });
        });
    }

    group.finish();
}

#[cfg(feature = "polars")]
criterion_group!(
    benches,
    bench_to_dataframe,
    bench_from_dataframe,
    bench_roundtrip
);

#[cfg(not(feature = "polars"))]
criterion_group!(benches);

criterion_main!(benches);
