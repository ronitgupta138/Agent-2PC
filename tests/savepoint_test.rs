//! Integration tests for Nested Savepoints & Speculative Execution Engine.

use agent_2pc::adapters::{FsAdapter, ResourceManager};
use agent_2pc::coordinator::Coordinator;
use agent_2pc::domain::{ResourceIntent, TxState};
use std::fs;
use tempfile::tempdir;

#[test]
fn test_savepoint_creation_and_isolation() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");
    let mut coord = Coordinator::new(&tx_dir).unwrap();

    let tid = coord.begin(Some(10000)).unwrap();

    // Stage first file write
    let target1 = tmp.path().join("base.txt");
    let mut adapter1 = FsAdapter::new("fs_base");
    let intent1 = ResourceIntent::FsWrite {
        path: target1,
        content: "base content".into(),
        create_dirs: true,
    };
    let _ = coord.prepare_participant(&tid, &mut adapter1, intent1).unwrap();

    // Create savepoint
    let sp1 = coord.savepoint(&tid, "sp1").unwrap();
    assert_eq!(sp1.name, "sp1");
    assert_eq!(sp1.participant_count, 1);
    assert_eq!(sp1.receipt_count, 1);

    // Creating duplicate savepoint name fails
    let duplicate_err = coord.savepoint(&tid, "sp1");
    assert!(duplicate_err.is_err());
}

#[test]
fn test_speculative_execution_and_rollback_to_savepoint() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");
    let mut coord = Coordinator::new(&tx_dir).unwrap();

    let tid = coord.begin(Some(10000)).unwrap();

    // Step 1: Base edit
    let file1 = tmp.path().join("file1.txt");
    let mut adapter1 = FsAdapter::new("fs_1");
    let intent1 = ResourceIntent::FsWrite {
        path: file1.clone(),
        content: "file 1 permanent".into(),
        create_dirs: true,
    };
    let _ = coord.prepare_participant(&tid, &mut adapter1, intent1).unwrap();

    // Checkpoint verified base state
    let sp_base = coord.savepoint(&tid, "sp_base").unwrap();
    assert_eq!(sp_base.name, "sp_base");

    // Step 2: Speculative branch (risky mutation)
    let file_speculative = tmp.path().join("speculative.txt");
    let mut adapter2 = FsAdapter::new("fs_speculative");
    let intent2 = ResourceIntent::FsWrite {
        path: file_speculative.clone(),
        content: "broken draft should not exist".into(),
        create_dirs: true,
    };
    let _ = coord.prepare_participant(&tid, &mut adapter2, intent2).unwrap();

    let tx_mid = coord.get_status(&tid).unwrap();
    assert_eq!(tx_mid.participants.len(), 2);
    assert_eq!(tx_mid.receipts.len(), 2);

    // Speculative test failed -> unwind back to sp_base!
    let mut resources: Vec<Box<dyn ResourceManager>> = vec![
        Box::new(adapter1),
        Box::new(adapter2),
    ];
    let unwound = coord.rollback_to(&tid, "sp_base", &mut resources[..]).unwrap();
    assert_eq!(unwound, 1);

    // Verify transaction state after savepoint rollback
    let tx_after = coord.get_status(&tid).unwrap();
    assert_eq!(tx_after.participants.len(), 1);
    assert_eq!(tx_after.receipts.len(), 1);
    assert_eq!(tx_after.state, TxState::Prepared);

    // Step 3: Stage alternative valid change
    let file3 = tmp.path().join("file3.txt");
    let mut adapter3 = FsAdapter::new("fs_3");
    let intent3 = ResourceIntent::FsWrite {
        path: file3.clone(),
        content: "file 3 valid alternative".into(),
        create_dirs: true,
    };
    let _ = coord.prepare_participant(&tid, &mut adapter3, intent3).unwrap();

    // Commit transaction
    let mut commit_resources: Vec<Box<dyn ResourceManager>> = vec![
        Box::new(FsAdapter::new("fs_1")),
        Box::new(adapter3),
    ];
    coord.commit(&tid, &mut commit_resources[..]).unwrap();

    // Assert file1 and file3 exist, while speculative.txt was never materialized
    assert_eq!(fs::read_to_string(&file1).unwrap(), "file 1 permanent");
    assert_eq!(fs::read_to_string(&file3).unwrap(), "file 3 valid alternative");
    assert!(!file_speculative.exists(), "Speculative file must not exist in workspace!");
}

#[test]
fn test_savepoint_wal_replay_and_crash_recovery() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");

    let tid;
    let file_kept = tmp.path().join("kept.txt");
    let file_unwound = tmp.path().join("unwound.txt");

    {
        let mut coord = Coordinator::new(&tx_dir).unwrap();
        tid = coord.begin(Some(10000)).unwrap();

        let mut a1 = FsAdapter::new("a1");
        let intent1 = ResourceIntent::FsWrite {
            path: file_kept.clone(),
            content: "kept".into(),
            create_dirs: true,
        };
        let _ = coord.prepare_participant(&tid, &mut a1, intent1).unwrap();

        coord.savepoint(&tid, "safe_point").unwrap();

        let mut a2 = FsAdapter::new("a2");
        let intent2 = ResourceIntent::FsWrite {
            path: file_unwound.clone(),
            content: "unwound".into(),
            create_dirs: true,
        };
        let _ = coord.prepare_participant(&tid, &mut a2, intent2).unwrap();

        let mut res: Vec<Box<dyn ResourceManager>> = vec![Box::new(a1), Box::new(a2)];
        coord.rollback_to(&tid, "safe_point", &mut res[..]).unwrap();

        // Drop coordinator to simulate crash after savepoint rollback
    }

    // Replay WAL on fresh coordinator boot
    let mut recovered = Coordinator::new(&tx_dir).unwrap();
    let tx = recovered.get_status(&tid).unwrap();

    // Only a1 remains in the participant list
    assert_eq!(tx.participants.len(), 1);
    assert_eq!(tx.participants[0].0, "a1");
    assert_eq!(tx.savepoints.len(), 1);
    assert_eq!(tx.savepoints[0].name, "safe_point");

    // Commit surviving participant
    let mut commit_res: Vec<Box<dyn ResourceManager>> = vec![Box::new(FsAdapter::new("a1"))];
    recovered.commit(&tid, &mut commit_res[..]).unwrap();

    assert!(file_kept.exists());
    assert!(!file_unwound.exists());
}

#[test]
fn test_nested_multiple_savepoints() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");
    let mut coord = Coordinator::new(&tx_dir).unwrap();

    let tid = coord.begin(Some(15000)).unwrap();

    // 1. Stage A -> SP1
    let file_a = tmp.path().join("a.txt");
    let mut a = FsAdapter::new("a");
    let _ = coord.prepare_participant(
        &tid,
        &mut a,
        ResourceIntent::FsWrite {
            path: file_a.clone(),
            content: "A".into(),
            create_dirs: true,
        },
    ).unwrap();
    coord.savepoint(&tid, "sp1").unwrap();

    // 2. Stage B -> SP2
    let file_b = tmp.path().join("b.txt");
    let mut b = FsAdapter::new("b");
    let _ = coord.prepare_participant(
        &tid,
        &mut b,
        ResourceIntent::FsWrite {
            path: file_b.clone(),
            content: "B".into(),
            create_dirs: true,
        },
    ).unwrap();
    coord.savepoint(&tid, "sp2").unwrap();

    // 3. Stage C
    let file_c = tmp.path().join("c.txt");
    let mut c = FsAdapter::new("c");
    let _ = coord.prepare_participant(
        &tid,
        &mut c,
        ResourceIntent::FsWrite {
            path: file_c.clone(),
            content: "C".into(),
            create_dirs: true,
        },
    ).unwrap();

    let mut res_all: Vec<Box<dyn ResourceManager>> = vec![
        Box::new(a),
        Box::new(b),
        Box::new(c),
    ];

    // Rollback to SP2 (unwinds C)
    let unwound = coord.rollback_to(&tid, "sp2", &mut res_all[..]).unwrap();
    assert_eq!(unwound, 1);
    assert_eq!(coord.get_status(&tid).unwrap().participants.len(), 2);

    // Rollback to SP1 (unwinds B)
    let unwound2 = coord.rollback_to(&tid, "sp1", &mut res_all[..]).unwrap();
    assert_eq!(unwound2, 1);
    assert_eq!(coord.get_status(&tid).unwrap().participants.len(), 1);

    // Commit remaining A
    let mut res_final: Vec<Box<dyn ResourceManager>> = vec![Box::new(FsAdapter::new("a"))];
    coord.commit(&tid, &mut res_final[..]).unwrap();

    assert!(file_a.exists());
    assert!(!file_b.exists());
    assert!(!file_c.exists());
}
