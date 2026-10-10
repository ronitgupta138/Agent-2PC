use agent_2pc::adapters::{ApiAdapter, CommandAdapter, FsAdapter, ResourceManager};
use agent_2pc::coordinator::Coordinator;
use agent_2pc::domain::{
    ParticipantId, ParticipantVote, ResourceIntent, TransactionId, TxError, TxState,
};
use agent_2pc::wal::{WalPayload, WalScanner, WalWriter};
use chrono::Utc;
use std::collections::HashMap;
use std::fs;
use tempfile::tempdir;

#[test]
fn test_wal_framing_and_crc32_tamper_detection() {
    let tmp = tempdir().unwrap();
    let wal_path = tmp.path().join("wal.bin");

    let mut writer = WalWriter::open(&wal_path).unwrap();
    let tid = TransactionId::new();

    writer
        .append(&WalPayload::Begin {
            tid,
            timestamp: Utc::now(),
            timeout_ms: 10_000,
        })
        .unwrap();

    writer
        .append(&WalPayload::Enlist {
            tid,
            participant_id: ParticipantId::new("fs_auth"),
            intent: ResourceIntent::FsWrite {
                path: tmp.path().join("auth.rs"),
                content: "pub fn login() {}".into(),
                create_dirs: true,
            },
        })
        .unwrap();

    let records = WalScanner::read_all(&wal_path).unwrap();
    assert_eq!(records.len(), 2);
    assert!(records[0].crc32 != 0);

    // Tamper with WAL file: flip a byte in the payload
    let mut raw_bytes = fs::read(&wal_path).unwrap();
    let target_idx = raw_bytes.len() - 5;
    raw_bytes[target_idx] ^= 0xFF; // Invert bits
    fs::write(&wal_path, raw_bytes).unwrap();

    // Verify scanner detects corruption via CRC32
    let scan_res = WalScanner::read_all(&wal_path);
    assert!(scan_res.is_err());
    match scan_res.unwrap_err() {
        TxError::ChecksumMismatch { .. } => {}
        other => panic!("Expected ChecksumMismatch, got: {:?}", other),
    }
}

#[test]
fn test_2pc_all_prepared_atomic_commit() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");
    let target_file_1 = tmp.path().join("src").join("user.rs");
    let target_file_2 = tmp.path().join("config.json");

    let mut coord = Coordinator::new(&tx_dir).unwrap();
    let tid = coord.begin(Some(30_000)).unwrap();

    let mut fs1 = FsAdapter::new("fs_user");
    let mut fs2 = FsAdapter::new("fs_config");
    let mut cmd = CommandAdapter::new("cmd_lint");

    // Phase 1: Prepare all 3 participants
    let vote1 = coord
        .prepare_participant(
            &tid,
            &mut fs1,
            ResourceIntent::FsWrite {
                path: target_file_1.clone(),
                content: "// User Model v1".into(),
                create_dirs: true,
            },
        )
        .unwrap();
    assert!(matches!(vote1, ParticipantVote::Prepared(_)));

    let vote2 = coord
        .prepare_participant(
            &tid,
            &mut fs2,
            ResourceIntent::FsWrite {
                path: target_file_2.clone(),
                content: r#"{"env": "staging"}"#.into(),
                create_dirs: false,
            },
        )
        .unwrap();
    assert!(matches!(vote2, ParticipantVote::Prepared(_)));

    let vote3 = coord
        .prepare_participant(
            &tid,
            &mut cmd,
            ResourceIntent::Command {
                command: "echo 'lint passed'".into(),
                working_dir: tmp.path().to_path_buf(),
                env: HashMap::new(),
            },
        )
        .unwrap();
    assert!(matches!(vote3, ParticipantVote::Prepared(_)));

    // BEFORE Phase 2 Commit: Target files must NOT exist on disk!
    assert!(!target_file_1.exists(), "Target 1 leaked before commit");
    assert!(!target_file_2.exists(), "Target 2 leaked before commit");

    // Phase 2: Commit
    let mut adapters: Vec<Box<dyn ResourceManager>> = vec![
        Box::new(fs1),
        Box::new(fs2),
        Box::new(cmd),
    ];
    coord.commit(&tid, &mut adapters[..]).unwrap();

    // AFTER Phase 2: Target files exist with exact content
    assert!(target_file_1.exists());
    assert_eq!(fs::read_to_string(&target_file_1).unwrap(), "// User Model v1");

    assert!(target_file_2.exists());
    assert_eq!(fs::read_to_string(&target_file_2).unwrap(), r#"{"env": "staging"}"#);

    // Assert status is COMMITTED
    let status = coord.get_status(&tid).unwrap();
    assert_eq!(status.state, TxState::Committed);
}

#[test]
fn test_2pc_partial_failure_deterministic_rollback() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");
    let critical_file = tmp.path().join("production.env");

    // Create preexisting file with safe content
    fs::write(&critical_file, "SECRET_KEY=SAFE_INITIAL_STATE").unwrap();

    let mut coord = Coordinator::new(&tx_dir).unwrap();
    let tid = coord.begin(Some(30_000)).unwrap();

    let mut fs_adapter = FsAdapter::new("fs_env");
    let mut cmd_failing = CommandAdapter::new("cmd_failing_migration");

    // Phase 1 Step 1: Stage overwrite of critical file
    let vote1 = coord
        .prepare_participant(
            &tid,
            &mut fs_adapter,
            ResourceIntent::FsWrite {
                path: critical_file.clone(),
                content: "SECRET_KEY=CORRUPTED_LEAKED_KEY".into(),
                create_dirs: false,
            },
        )
        .unwrap();
    assert!(matches!(vote1, ParticipantVote::Prepared(_)));

    // Phase 1 Step 2: Failing command (exit code 42)
    let vote2 = coord
        .prepare_participant(
            &tid,
            &mut cmd_failing,
            ResourceIntent::Command {
                command: "sh -c 'exit 42'".into(),
                working_dir: tmp.path().to_path_buf(),
                env: HashMap::new(),
            },
        )
        .unwrap();
    assert!(matches!(vote2, ParticipantVote::Aborted { .. }));

    // Attempting to commit MUST fail and trigger global abort
    let mut adapters: Vec<Box<dyn ResourceManager>> = vec![
        Box::new(fs_adapter),
        Box::new(cmd_failing),
    ];
    let commit_res = coord.commit(&tid, &mut adapters[..]);
    assert!(commit_res.is_err(), "Commit succeeded despite failed vote");

    // CRITICAL INVARIANT: Preexisting file MUST still hold safe content!
    assert_eq!(
        fs::read_to_string(&critical_file).unwrap(),
        "SECRET_KEY=SAFE_INITIAL_STATE"
    );

    // State MUST be ABORTED
    let status = coord.get_status(&tid).unwrap();
    assert_eq!(status.state, TxState::Aborted);
}

#[test]
fn test_crash_recovery_uncommitted_rollback() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");
    let test_file = tmp.path().join("db.schema");

    fs::write(&test_file, "CREATE TABLE users;").unwrap();

    let tid = {
        let mut coord = Coordinator::new(&tx_dir).unwrap();
        let tid = coord.begin(Some(10_000)).unwrap();
        let mut fs_adapter = FsAdapter::new("fs_schema");

        let _ = coord.prepare_participant(
            &tid,
            &mut fs_adapter,
            ResourceIntent::FsWrite {
                path: test_file.clone(),
                content: "DROP TABLE users;".into(),
                create_dirs: false,
            },
        );

        // Process "crashes" here before commit decision is made
        tid
    };

    // New coordinator boots on next session
    let mut new_coord = Coordinator::new(&tx_dir).unwrap();
    let status = new_coord.get_status(&tid).unwrap();
    assert_eq!(status.state, TxState::Prepared);

    // Run recovery
    let mut adapters: Vec<Box<dyn ResourceManager>> = vec![Box::new(FsAdapter::new("fs_schema"))];
    let report = new_coord.recover(&mut adapters[..]).unwrap();

    assert!(report.rolled_back.contains(&tid));
    assert_eq!(
        fs::read_to_string(&test_file).unwrap(),
        "CREATE TABLE users;"
    );

    let final_status = new_coord.get_status(&tid).unwrap();
    assert_eq!(final_status.state, TxState::Aborted);
}

#[test]
fn test_api_adapter_lease_and_compensation() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");

    let mut coord = Coordinator::new(&tx_dir).unwrap();
    let tid = coord.begin(Some(15_000)).unwrap();

    let mut api_adapter = ApiAdapter::new("linear_issue_create");
    let vote = coord
        .prepare_participant(
            &tid,
            &mut api_adapter,
            ResourceIntent::RestApi {
                endpoint: "https://api.linear.app/graphql".into(),
                method: "POST".into(),
                headers: HashMap::new(),
                body: Some(r#"{"query": "mutation { issueCreate }"}"#.into()),
                idempotency_key: "idem-key-9988".into(),
                compensation_endpoint: Some("https://api.linear.app/graphql".into()),
                compensation_method: Some("POST".into()),
                compensation_body: Some(r#"{"query": "mutation { issueArchive }"}"#.into()),
            },
        )
        .unwrap();

    assert!(matches!(vote, ParticipantVote::Prepared(_)));
    if let ParticipantVote::Prepared(receipt) = vote {
        assert_eq!(receipt.idempotency_key, Some("idem-key-9988".into()));
    }

    let mut adapters: Vec<Box<dyn ResourceManager>> = vec![Box::new(api_adapter)];
    coord.commit(&tid, &mut adapters[..]).unwrap();

    let status = coord.get_status(&tid).unwrap();
    assert_eq!(status.state, TxState::Committed);
}

#[test]
fn test_parallel_prepare_with_short_circuit_abort() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");
    let test_file = tmp.path().join("parallel.txt");

    let mut coord = Coordinator::new(&tx_dir).unwrap();
    let tid = coord.begin(Some(30_000)).unwrap();

    let mut items: Vec<(Box<dyn ResourceManager>, ResourceIntent)> = vec![
        (
            Box::new(FsAdapter::new("fs_parallel_1")),
            ResourceIntent::FsWrite {
                path: test_file.clone(),
                content: "Parallel Write Success".into(),
                create_dirs: false,
            },
        ),
        (
            Box::new(CommandAdapter::new("cmd_failing_parallel")),
            ResourceIntent::Command {
                command: "sh -c 'echo \"parallel failure\" >&2; exit 99'".into(),
                working_dir: tmp.path().to_path_buf(),
                env: HashMap::new(),
            },
        ),
        (
            Box::new(FsAdapter::new("fs_parallel_2")),
            ResourceIntent::FsWrite {
                path: tmp.path().join("parallel_2.txt"),
                content: "Another file".into(),
                create_dirs: false,
            },
        ),
    ];

    let votes = coord.prepare_batch_parallel(&tid, &mut items[..]).unwrap();
    assert_eq!(votes.len(), 3);

    // Verify at least one vote is Aborted
    let any_aborted = votes.iter().any(|v| matches!(v, ParticipantVote::Aborted { .. }));
    assert!(any_aborted);

    // Attempting commit must fail and trigger rollback
    let mut adapters: Vec<Box<dyn ResourceManager>> = items.into_iter().map(|(r, _)| r).collect();
    let commit_res = coord.commit(&tid, &mut adapters[..]);
    assert!(commit_res.is_err());

    // File must NOT exist
    assert!(!test_file.exists());

    let final_status = coord.get_status(&tid).unwrap();
    assert_eq!(final_status.state, TxState::Aborted);
}

#[test]
fn test_reflink_or_copy_snapshot_staging() {
    let tmp = tempdir().unwrap();
    let src_file = tmp.path().join("source.txt");
    let clone_file = tmp.path().join("clone.txt");

    fs::write(&src_file, "Reflink zero-copy test payload").unwrap();
    agent_2pc::adapters::reflink_or_copy(&src_file, &clone_file).unwrap();

    assert!(clone_file.exists());
    assert_eq!(
        fs::read_to_string(&clone_file).unwrap(),
        "Reflink zero-copy test payload"
    );
}

#[test]
fn test_optimistic_concurrency_conflict_detection() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");
    let shared_file = tmp.path().join("schema.sql");

    // 1. Initial shared file state
    fs::write(&shared_file, "CREATE TABLE orders;").unwrap();

    let mut coord = Coordinator::new(&tx_dir).unwrap();
    let tid = coord.begin(Some(30_000)).unwrap();

    // 2. Agent records read of shared_file (OCC read-set fingerprint)
    let initial_hash = coord.record_read(&tid, &shared_file).unwrap();
    assert_ne!(initial_hash, 0);

    // 3. Concurrent process modifies shared_file behind the agent's back!
    fs::write(&shared_file, "CREATE TABLE orders_concurrently_mutated;").unwrap();

    // 4. Agent attempts to prepare file update
    let mut fs_adapter = FsAdapter::new("fs_agent_orders");
    let vote = coord
        .prepare_participant(
            &tid,
            &mut fs_adapter,
            ResourceIntent::FsWrite {
                path: shared_file.clone(),
                content: "CREATE TABLE orders_agent_staged;".into(),
                create_dirs: false,
            },
        )
        .unwrap();

    // 5. Must vote ABORT due to conflict detection!
    assert!(matches!(vote, ParticipantVote::Aborted { .. }));
    if let ParticipantVote::Aborted { reason } = vote {
        assert!(reason.contains("Conflict detected"));
    }

    // Status must be Aborting / Aborted
    let status = coord.get_status(&tid).unwrap();
    assert_eq!(status.state, TxState::Aborting);
}

#[test]
fn test_monitor_stats_collection() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");

    let mut coord = Coordinator::new(&tx_dir).unwrap();
    let tid = coord.begin(Some(20_000)).unwrap();

    let monitor = agent_2pc::monitor::Monitor::new(&tx_dir);
    let stats = monitor.collect_stats().unwrap();

    assert_eq!(stats.active_count, 1);
    assert_eq!(stats.active_transactions[0].tid, tid.to_string());
    assert!(stats.health_status.contains("HEALTHY"));
}

#[test]
fn test_sandbox_adapter_filesystem_protection() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");

    let mut coord = Coordinator::new(&tx_dir).unwrap();
    let tid = coord.begin(Some(30_000)).unwrap();

    let mut sandbox = agent_2pc::sandbox::SandboxAdapter::new("sandbox_guard");

    // 1. Benign command inside tmp working directory succeeds
    let vote = coord
        .prepare_participant(
            &tid,
            &mut sandbox,
            ResourceIntent::SandboxCommand {
                command: "echo 'sandbox operational'".into(),
                working_dir: tmp.path().to_path_buf(),
                writable_paths: vec![tmp.path().to_path_buf()],
                env: HashMap::new(),
            },
        )
        .unwrap();

    assert!(matches!(vote, ParticipantVote::Prepared(_)));
}

#[test]
fn test_governor_adapter_timeout_containment() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");

    let mut coord = Coordinator::new(&tx_dir).unwrap();
    let tid = coord.begin(Some(30_000)).unwrap();

    let mut gov = agent_2pc::governor::GovernorAdapter::new("gov_guard");

    // Command tries to sleep 5 seconds, but governor enforces strict 100ms velocity limit
    let vote = coord
        .prepare_participant(
            &tid,
            &mut gov,
            ResourceIntent::GovernedCommand {
                command: "sleep 5".into(),
                working_dir: tmp.path().to_path_buf(),
                max_forks: 20,
                timeout_ms: 100, // 100ms limit
                env: HashMap::new(),
            },
        )
        .unwrap();

    assert!(matches!(vote, ParticipantVote::Aborted { .. }));
    if let ParticipantVote::Aborted { reason } = vote {
        assert!(reason.contains("Velocity Governor Tripped"));
    }
}

#[test]
fn test_governor_adapter_clean_execution() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");

    let mut coord = Coordinator::new(&tx_dir).unwrap();
    let tid = coord.begin(Some(30_000)).unwrap();

    let mut gov = agent_2pc::governor::GovernorAdapter::new("gov_safe");

    let vote = coord
        .prepare_participant(
            &tid,
            &mut gov,
            ResourceIntent::GovernedCommand {
                command: "echo 'safe speed'".into(),
                working_dir: tmp.path().to_path_buf(),
                max_forks: 50,
                timeout_ms: 5000,
                env: HashMap::new(),
            },
        )
        .unwrap();

    assert!(matches!(vote, ParticipantVote::Prepared(_)));
}

#[test]
fn test_microvm_adapter_execution() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");

    let mut coord = Coordinator::new(&tx_dir).unwrap();
    let tid = coord.begin(Some(30_000)).unwrap();

    let mut vm = agent_2pc::microvm::MicrovmAdapter::new("vm_runner");

    let vote = coord
        .prepare_participant(
            &tid,
            &mut vm,
            ResourceIntent::MicrovmCommand {
                command: "echo 'microvm hardware boundary active'".into(),
                working_dir: tmp.path().to_path_buf(),
                vcpus: 1,
                memory_mb: 128,
                env: HashMap::new(),
            },
        )
        .unwrap();

    assert!(matches!(vote, ParticipantVote::Prepared(_)));
    if let ParticipantVote::Prepared(receipt) = vote {
        assert_eq!(receipt.metadata.get("memory_mb"), Some(&"128".to_string()));
    }
}
