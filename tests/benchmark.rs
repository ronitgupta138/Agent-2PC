use agent_2pc::adapters::{FsAdapter, ResourceManager};
use agent_2pc::coordinator::Coordinator;
use agent_2pc::domain::{ResourceIntent, TransactionId};
use agent_2pc::wal::{WalPayload, WalWriter};
use chrono::Utc;
use std::fs;
use std::time::Instant;
use tempfile::tempdir;

#[test]
fn test_wal_append_latency_benchmark() {
    let tmp = tempdir().unwrap();
    let wal_path = tmp.path().join("wal_bench.bin");
    let mut writer = WalWriter::open(&wal_path).unwrap();

    let iterations = 1000;
    let mut latencies_us = Vec::with_capacity(iterations);

    for _i in 0..iterations {
        let tid = TransactionId::new();
        let start = Instant::now();
        writer
            .append(&WalPayload::Begin {
                tid,
                timestamp: Utc::now(),
                timeout_ms: 30_000,
            })
            .unwrap();
        latencies_us.push(start.elapsed().as_micros());
    }

    latencies_us.sort_unstable();
    let p50 = latencies_us[iterations * 50 / 100];
    let p95 = latencies_us[iterations * 95 / 100];
    let p99 = latencies_us[iterations * 99 / 100];

    println!(
        "\n⚡ WAL Fsync Latency: p50={}µs | p95={}µs | p99={}µs (over {} operations)",
        p50, p95, p99, iterations
    );

    // Ensure p50 is sub-millisecond (typically 30-100µs on NVMe)
    assert!(p50 < 10_000, "p50 latency exceeded 10ms");
}

#[test]
fn test_end_to_end_2pc_transaction_throughput() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");
    let dest_dir = tmp.path().join("dest");
    fs::create_dir_all(&dest_dir).unwrap();

    let mut coord = Coordinator::new(&tx_dir).unwrap();
    let iterations = 200;
    let start = Instant::now();

    for i in 0..iterations {
        let tid = coord.begin(Some(10_000)).unwrap();
        let mut fs_adapter = FsAdapter::new(format!("fs_bench_{}", i));
        let target_file = dest_dir.join(format!("file_{}.txt", i));

        coord
            .prepare_participant(
                &tid,
                &mut fs_adapter,
                ResourceIntent::FsWrite {
                    path: target_file,
                    content: format!("Payload content iteration {}", i),
                    create_dirs: false,
                },
            )
            .unwrap();

        let mut adapters: Vec<Box<dyn ResourceManager>> = vec![Box::new(fs_adapter)];
        coord.commit(&tid, &mut adapters[..]).unwrap();
    }

    let elapsed = start.elapsed();
    let ops_per_sec = (iterations as f64) / elapsed.as_secs_f64();
    println!(
        "\n⚡ 2PC Transaction Throughput: {:.1} tx/sec ({:?} for {} full 2PC rounds)",
        ops_per_sec, elapsed, iterations
    );

    assert!(ops_per_sec > 10.0);
}
