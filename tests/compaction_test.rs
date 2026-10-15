//! Integration tests for WAL Checkpoint Compaction Engine.

use agent_2pc::adapters::{FsAdapter, ResourceManager};
use agent_2pc::coordinator::Coordinator;
use agent_2pc::domain::{ResourceIntent, TxState};
use agent_2pc::wal::{WalPayload, WalScanner};
use std::fs;
use tempfile::tempdir;

#[test]
fn test_compaction_purges_committed_and_aborted() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");
    let mut coord = Coordinator::new(&tx_dir).unwrap();

    let target_file = tmp.path().join("file.txt");

    // Run 5 committed transactions
    for i in 0..5 {
        let tid = coord.begin(Some(10000)).unwrap();
        let mut adapter = FsAdapter::new(format!("fs_{}", i));
        let intent = ResourceIntent::FsWrite {
            path: target_file.clone(),
            content: format!("Content iteration {}", i),
            create_dirs: true,
        };
        let _ = coord.prepare_participant(&tid, &mut adapter, intent).unwrap();
        let mut resources: Vec<Box<dyn ResourceManager>> = vec![Box::new(adapter)];
        coord.commit(&tid, &mut resources[..]).unwrap();
    }

    // Run 5 aborted transactions
    for i in 5..10 {
        let tid = coord.begin(Some(10000)).unwrap();
        let mut adapter = FsAdapter::new(format!("fs_{}", i));
        let intent = ResourceIntent::FsWrite {
            path: target_file.clone(),
            content: format!("Aborted iteration {}", i),
            create_dirs: true,
        };
        let _ = coord.prepare_participant(&tid, &mut adapter, intent).unwrap();
        let mut resources: Vec<Box<dyn ResourceManager>> = vec![Box::new(adapter)];
        coord.abort(&tid, "Test abort", &mut resources[..]).unwrap();
    }

    let wal_path = tx_dir.join("wal.bin");
    let pre_records = WalScanner::read_all(&wal_path).unwrap();
    assert!(pre_records.len() >= 30, "Expected at least 30 historical WAL frames");

    // Execute WAL compaction
    let report = coord.compact().unwrap();

    assert_eq!(report.active_transactions, 0);
    assert_eq!(report.purged_transactions, 10);
    assert!(report.compacted_bytes < report.original_bytes);
    assert_eq!(report.compacted_records, 1); // Only the Checkpoint marker remains

    // Verify disk WAL contains Checkpoint frame
    let post_records = WalScanner::read_all(&wal_path).unwrap();
    assert_eq!(post_records.len(), 1);
    match &post_records[0].payload {
        WalPayload::Checkpoint { active_tids, .. } => {
            assert!(active_tids.is_empty());
        }
        _ => panic!("Expected Checkpoint record as first frame in compacted WAL"),
    }

    // Ensure Coordinator in-memory table is clear of finalized transactions
    assert_eq!(coord.active_transactions().len(), 0);
}

#[test]
fn test_compaction_preserves_in_flight_transactions() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");
    let mut coord = Coordinator::new(&tx_dir).unwrap();

    let target_file = tmp.path().join("in_flight.txt");
    fs::write(&target_file, "initial").unwrap();

    // 1. Transaction to be committed before compaction
    let tid_done = coord.begin(Some(10000)).unwrap();
    let mut adapter_done = FsAdapter::new("fs_done");
    let intent_done = ResourceIntent::FsWrite {
        path: target_file.clone(),
        content: "done".into(),
        create_dirs: true,
    };
    let _ = coord.prepare_participant(&tid_done, &mut adapter_done, intent_done).unwrap();
    let mut res_done: Vec<Box<dyn ResourceManager>> = vec![Box::new(adapter_done)];
    coord.commit(&tid_done, &mut res_done[..]).unwrap();

    // 2. In-flight Transaction 1: Active with OCC read-set
    let tid_active = coord.begin(Some(15000)).unwrap();
    coord.record_read(&tid_active, &target_file).unwrap();

    // 3. In-flight Transaction 2: Prepared with staged write
    let tid_prep = coord.begin(Some(20000)).unwrap();
    let mut adapter_prep = FsAdapter::new("fs_prep");
    let final_dest = tmp.path().join("final.txt");
    let intent_prep = ResourceIntent::FsWrite {
        path: final_dest.clone(),
        content: "persisted across compaction".into(),
        create_dirs: true,
    };
    let _ = coord.prepare_participant(&tid_prep, &mut adapter_prep, intent_prep).unwrap();

    // Perform compaction while tid_active and tid_prep are in flight
    let report = coord.compact().unwrap();
    assert_eq!(report.active_transactions, 2);
    assert_eq!(report.purged_transactions, 1);

    // Verify both in-flight transactions survived in coordinator
    assert_eq!(coord.active_transactions().len(), 2);
    let active_tx = coord.get_status(&tid_active).unwrap();
    assert_eq!(active_tx.state, TxState::Active);
    assert!(active_tx.read_set.contains_key(&target_file));

    let prep_tx = coord.get_status(&tid_prep).unwrap();
    assert_eq!(prep_tx.state, TxState::Prepared);
    assert_eq!(prep_tx.receipts.len(), 1);

    // Verify we can successfully commit tid_prep from the compacted state
    let mut res_prep: Vec<Box<dyn ResourceManager>> = vec![Box::new(adapter_prep)];
    coord.commit(&tid_prep, &mut res_prep[..]).unwrap();

    assert_eq!(fs::read_to_string(&final_dest).unwrap(), "persisted across compaction");
}

#[test]
fn test_crash_recovery_from_compacted_wal() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");

    let tid_dangling;
    {
        let mut coord = Coordinator::new(&tx_dir).unwrap();
        tid_dangling = coord.begin(Some(10000)).unwrap();

        let staged_file = tmp.path().join("staged_uncommitted.txt");
        let mut adapter = FsAdapter::new("fs_uncommitted");
        let intent = ResourceIntent::FsWrite {
            path: staged_file.clone(),
            content: "should be rolled back".into(),
            create_dirs: true,
        };
        let _ = coord.prepare_participant(&tid_dangling, &mut adapter, intent).unwrap();

        // Compact while transaction is prepared
        let report = coord.compact().unwrap();
        assert_eq!(report.active_transactions, 1);

        // Crash simulation: Coordinator dropped without commit
    }

    // Reopen fresh coordinator on disk
    let mut recovered_coord = Coordinator::new(&tx_dir).unwrap();
    assert_eq!(recovered_coord.active_transactions().len(), 1);

    let mut dummy_adapters: Vec<Box<dyn ResourceManager>> = vec![Box::new(FsAdapter::new("fs_uncommitted"))];
    let recovery_report = recovered_coord.recover(&mut dummy_adapters[..]).unwrap();

    assert_eq!(recovery_report.rolled_back.len(), 1);
    assert_eq!(recovery_report.rolled_back[0], tid_dangling);
    assert_eq!(recovered_coord.active_transactions().len(), 0);
}

#[test]
fn test_compactor_offline_compact_path() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");

    // Initialize workspace and run an aborted transaction
    {
        let mut coord = Coordinator::new(&tx_dir).unwrap();
        let tid = coord.begin(Some(5000)).unwrap();
        let mut adapter = FsAdapter::new("fs_offline");
        let path = tmp.path().join("offline.txt");
        let intent = ResourceIntent::FsWrite {
            path,
            content: "offline".into(),
            create_dirs: true,
        };
        let _ = coord.prepare_participant(&tid, &mut adapter, intent).unwrap();
        let mut res: Vec<Box<dyn ResourceManager>> = vec![Box::new(adapter)];
        coord.abort(&tid, "Abort offline", &mut res[..]).unwrap();
    }

    // Call compact_path directly without active Coordinator instance
    let report = agent_2pc::compactor::Compactor::compact_path(&tx_dir).unwrap();
    assert_eq!(report.active_transactions, 0);
    assert_eq!(report.purged_transactions, 1);
    assert_eq!(report.compacted_records, 1);
}
