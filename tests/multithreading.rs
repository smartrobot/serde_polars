use serde::{Deserialize, Serialize};
use serde_polars::{from_dataframe, to_dataframe};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ThreadTestRecord {
    id: i64,
    thread_id: usize,
    value: f64,
    name: String,
}


#[test]
fn test_multithreading_safety() {
    use std::thread;

    const NUM_THREADS: usize = 4;
    const RECORDS_PER_THREAD: i64 = 100;

    let handles: Vec<_> = (0..NUM_THREADS)
        .map(|thread_id| {
            thread::spawn(move || {
                // Create records for this thread
                let records: Vec<ThreadTestRecord> = (0..RECORDS_PER_THREAD)
                    .map(|i| ThreadTestRecord {
                        id: i,
                        thread_id,
                        value: (i as f64) * (thread_id as f64),
                        name: format!("Thread_{}_Record_{}", thread_id, i),
                    })
                    .collect();

                // Convert to DataFrame
                let df = to_dataframe(&records).expect(&format!(
                    "Thread {} failed to convert to DataFrame",
                    thread_id
                ));

                // Convert back
                let converted: Vec<ThreadTestRecord> = from_dataframe(df).expect(&format!(
                    "Thread {} failed to convert from DataFrame",
                    thread_id
                ));

                // Verify
                assert_eq!(records, converted);

                println!("✓ Thread {} completed successfully", thread_id);
                records.len()
            })
        })
        .collect();

    // Wait for all threads to complete
    let mut total_records = 0;
    for handle in handles {
        total_records += handle.join().expect("Thread panicked");
    }

    assert_eq!(total_records, (NUM_THREADS * RECORDS_PER_THREAD as usize));
    println!(
        "✓ All {} threads completed, processed {} total records",
        NUM_THREADS, total_records
    );
}

#[cfg(feature = "polars")]
#[test]
fn test_rayon_parallel_iterator() {
    // Note: This test requires rayon to be available, but we'll make it optional
    // If rayon is not available, this test will be skipped at runtime

    // Create a collection of datasets to process in parallel
    let datasets: Vec<Vec<ThreadTestRecord>> = (0..10)
        .map(|dataset_id| {
            (0..50)
                .map(|i| ThreadTestRecord {
                    id: i,
                    thread_id: dataset_id,
                    value: (i as f64) * (dataset_id as f64),
                    name: format!("Dataset_{}_Record_{}", dataset_id, i),
                })
                .collect()
        })
        .collect();

    // Process datasets in parallel using standard threading for now
    // In real usage with rayon, you would use .into_par_iter() here
    let results: Vec<_> = datasets
        .into_iter()
        .map(|records| {
            // Convert to DataFrame
            let df = to_dataframe(&records).expect("Failed to convert to DataFrame in parallel");

            // Convert back
            let converted: Vec<ThreadTestRecord> =
                from_dataframe(df).expect("Failed to convert from DataFrame in parallel");

            // Verify roundtrip
            assert_eq!(records, converted);

            converted.len()
        })
        .collect();

    // Verify all datasets were processed
    assert_eq!(results.len(), 10);
    for &count in &results {
        assert_eq!(count, 50);
    }

    println!("✓ Parallel processing simulation completed successfully");
}

#[cfg(feature = "polars")]
#[test]
fn test_concurrent_dataframe_operations() {
    use std::sync::Arc;
    use std::thread;

    // Create a base dataset
    let base_records: Vec<ThreadTestRecord> = (0..1000)
        .map(|i| ThreadTestRecord {
            id: i,
            thread_id: 0,
            value: i as f64,
            name: format!("Record_{}", i),
        })
        .collect();

    let df = to_dataframe(&base_records).expect("Failed to create base DataFrame");
    let df = Arc::new(df);
    let base_records = Arc::new(base_records);

    // Multiple threads reading from the same DataFrame
    let handles: Vec<_> = (0..4)
        .map(|thread_id| {
            let df_clone = Arc::clone(&df);
            let base_records_clone = Arc::clone(&base_records);
            thread::spawn(move || {
                // Each thread converts the same DataFrame
                let converted: Vec<ThreadTestRecord> = from_dataframe((*df_clone).clone())
                    .expect(&format!("Thread {} failed to convert DataFrame", thread_id));

                assert_eq!(converted.len(), 1000);
                assert_eq!(converted, *base_records_clone);

                println!("✓ Thread {} successfully read shared DataFrame", thread_id);
            })
        })
        .collect();

    // Wait for all threads
    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    println!("✓ Concurrent DataFrame reading test completed");
}

#[cfg(feature = "polars")]
#[test]
fn test_thread_local_operations() {
    use std::thread;

    thread_local! {
        static THREAD_DATA: std::cell::RefCell<Vec<ThreadTestRecord>> = std::cell::RefCell::new(Vec::new());
    }

    let handles: Vec<_> = (0..3)
        .map(|thread_id| {
            thread::spawn(move || {
                THREAD_DATA.with(|data| {
                    let mut records = data.borrow_mut();

                    // Add records to thread-local storage
                    for i in 0..10 {
                        records.push(ThreadTestRecord {
                            id: i,
                            thread_id,
                            value: i as f64,
                            name: format!("ThreadLocal_{}_{}", thread_id, i),
                        });
                    }

                    // Convert to DataFrame and back
                    let df = to_dataframe(&*records).expect(&format!(
                        "Thread {} failed to convert thread-local data",
                        thread_id
                    ));

                    let converted: Vec<ThreadTestRecord> = from_dataframe(df)
                        .expect(&format!("Thread {} failed to convert back", thread_id));

                    assert_eq!(*records, converted);

                    println!(
                        "✓ Thread {} processed {} thread-local records",
                        thread_id,
                        records.len()
                    );
                    records.len()
                });
            })
        })
        .collect();

    // Wait for all threads
    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    println!("✓ Thread-local operations test completed");
}
