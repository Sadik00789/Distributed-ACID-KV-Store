use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use storage::StorageEngine;
use tempfile::tempdir;
use txn_coordinator::{HybridLogicalClock, Mutation, TransactionCoordinator};

fn bench_2pc_throughput(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let engine = StorageEngine::open(dir.path()).unwrap();
    let hlc = HybridLogicalClock::new();
    let coordinator = TransactionCoordinator::new(engine, hlc.clone());

    let latencies = Mutex::new(Vec::<Duration>::new());

    c.bench_function("2pc_write_commit_cycle", |b| {
        let mut idx = 0u64;
        b.iter(|| {
            idx += 1;
            let key = format!("bench_key_{}", idx).into_bytes();
            let val = b"benchmark_payload_value".to_vec();
            let start_ts = hlc.now();

            let mutations = vec![Mutation::Put {
                key: key.clone(),
                value: val,
            }];

            let op_start = Instant::now();
            coordinator
                .prewrite(start_ts, &mutations, &key, 3000)
                .unwrap();
            let commit_ts = hlc.now();
            coordinator.commit(start_ts, commit_ts, &[key]).unwrap();
            let elapsed = op_start.elapsed();

            if let Ok(mut lock) = latencies.lock() {
                lock.push(elapsed);
            }
        });
    });

    let mut samples = latencies.into_inner().unwrap();
    if !samples.is_empty() {
        samples.sort();
        let len = samples.len();
        let p50 = samples[len * 50 / 100];
        let p99 = samples[len * 99 / 100];

        // Tukey's fences to identify outliers
        let q1 = samples[len * 25 / 100].as_nanos() as f64;
        let q3 = samples[len * 75 / 100].as_nanos() as f64;
        let iqr = q3 - q1;
        let upper_fence = q3 + 1.5 * iqr;
        let severe_fence = q3 + 3.0 * iqr;

        let outliers = samples.iter().filter(|d| d.as_nanos() as f64 > upper_fence).count();
        let severe_outliers = samples.iter().filter(|d| d.as_nanos() as f64 > severe_fence).count();

        let outlier_pct = (outliers as f64 / len as f64) * 100.0;
        let severe_outlier_pct = (severe_outliers as f64 / len as f64) * 100.0;

        println!("\n================ Latency Distribution & Outliers ================");
        println!("Total Operations        : {}", len);
        println!("p50 Latency            : {:?}", p50);
        println!("p99 Latency            : {:?}", p99);
        println!("Mild & Severe Outlier %: {:.2}%", outlier_pct);
        println!("Severe Outlier %        : {:.2}%", severe_outlier_pct);
        println!("=================================================================\n");
    }
}

criterion_group!(benches, bench_2pc_throughput);
criterion_main!(benches);
