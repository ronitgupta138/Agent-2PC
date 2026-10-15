use agent_2pc::adapters::{reconstruct_adapters, FsAdapter, ResourceManager};
use agent_2pc::bundle::BundleManager;
use agent_2pc::coordinator::Coordinator;
use agent_2pc::domain::{
    ParticipantId, ParticipantVote, ResourceIntent, StageReceipt, TransactionId, TxError, TxState,
};
use std::fs;
use std::path::Path;
use tempfile::tempdir;

struct PanickingAdapter {
    id: ParticipantId,
}

impl PanickingAdapter {
    fn new(id: impl Into<String>) -> Self {
        Self {
            id: ParticipantId::new(id),
        }
    }
}

impl ResourceManager for PanickingAdapter {
    fn id(&self) -> &ParticipantId {
        &self.id
    }

    fn prepare(
        &mut self,
        _tid: &TransactionId,
        _intent: &ResourceIntent,
        _scratch_dir: &Path,
    ) -> Result<ParticipantVote, TxError> {
        panic!("Intentional panic during prepare");
    }

    fn commit(&mut self, _tid: &TransactionId, _receipt: &StageReceipt) -> Result<(), TxError> {
        Ok(())
    }

    fn abort(&mut self, _tid: &TransactionId, _receipt: Option<&StageReceipt>) -> Result<(), TxError> {
        Ok(())
    }
}

#[test]
fn test_coordinator_parallel_prepare_panic_resilience() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");
    let mut coord = Coordinator::new(&tx_dir).unwrap();
    let tid = coord.begin(Some(10000)).unwrap();

    let mut items: Vec<(Box<dyn ResourceManager>, ResourceIntent)> = vec![
        (
            Box::new(FsAdapter::new("fs_normal")),
            ResourceIntent::FsWrite {
                path: tmp.path().join("normal.txt"),
                content: "ok".into(),
                create_dirs: false,
            },
        ),
        (
            Box::new(PanickingAdapter::new("panic_res")),
            ResourceIntent::Command {
                command: "true".into(),
                working_dir: tmp.path().to_path_buf(),
                env: std::collections::HashMap::new(),
            },
        ),
    ];

    let votes = coord.prepare_batch_parallel(&tid, &mut items[..]).unwrap();
    assert_eq!(votes.len(), 2);
    let any_aborted = votes.iter().any(|v| matches!(v, ParticipantVote::Aborted { .. }));
    assert!(any_aborted, "Panic must be converted to an Aborted vote");

    let status = coord.get_status(&tid).unwrap();
    assert_eq!(status.state, TxState::Aborting);
}

#[test]
fn test_commit_abort_idempotency() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");
    let mut coord = Coordinator::new(&tx_dir).unwrap();

    // 1. Commit idempotency
    let tid1 = coord.begin(Some(10000)).unwrap();
    let f1 = tmp.path().join("file1.txt");
    let mut a1 = FsAdapter::new("fs_1");
    coord
        .prepare_participant(
            &tid1,
            &mut a1,
            ResourceIntent::FsWrite {
                path: f1.clone(),
                content: "committed once".into(),
                create_dirs: false,
            },
        )
        .unwrap();

    let mut res1: Vec<Box<dyn ResourceManager>> = vec![Box::new(a1)];
    coord.commit(&tid1, &mut res1[..]).unwrap();
    assert_eq!(coord.get_status(&tid1).unwrap().state, TxState::Committed);

    // Repeated commit should be a clean Ok(()) no-op
    let mut res_repeat: Vec<Box<dyn ResourceManager>> = vec![Box::new(FsAdapter::new("fs_1"))];
    assert!(coord.commit(&tid1, &mut res_repeat[..]).is_ok());

    // Aborting an already committed transaction must return Err
    assert!(coord.abort(&tid1, "try abort", &mut res_repeat[..]).is_err());

    // 2. Abort idempotency
    let tid2 = coord.begin(Some(10000)).unwrap();
    let f2 = tmp.path().join("file2.txt");
    let mut a2 = FsAdapter::new("fs_2");
    coord
        .prepare_participant(
            &tid2,
            &mut a2,
            ResourceIntent::FsWrite {
                path: f2,
                content: "aborted".into(),
                create_dirs: false,
            },
        )
        .unwrap();

    let mut res2: Vec<Box<dyn ResourceManager>> = vec![Box::new(a2)];
    coord.abort(&tid2, "first abort", &mut res2[..]).unwrap();
    assert_eq!(coord.get_status(&tid2).unwrap().state, TxState::Aborted);

    // Repeated abort should be a clean Ok(()) no-op
    let mut res_repeat2: Vec<Box<dyn ResourceManager>> = vec![Box::new(FsAdapter::new("fs_2"))];
    assert!(coord.abort(&tid2, "second abort", &mut res_repeat2[..]).is_ok());

    // Committing an already aborted transaction must return Err
    assert!(coord.commit(&tid2, &mut res_repeat2[..]).is_err());
}

#[test]
fn test_bundle_path_traversal_prevention() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");
    let mut coord = Coordinator::new(&tx_dir).unwrap();

    let tid = TransactionId::new();

    let payload = agent_2pc::bundle::BundlePayload {
        tid,
        tx_state: agent_2pc::coordinator::ActiveTx {
            tid,
            state: TxState::Prepared,
            created_at: chrono::Utc::now(),
            timeout_ms: 10000,
            participants: vec![ParticipantId::new("malicious")],
            receipts: std::collections::HashMap::new(),
            read_set: std::collections::HashMap::new(),
            abort_reason: None,
            savepoints: Vec::new(),
        },
        scratch_files: {
            let mut m = std::collections::BTreeMap::new();
            m.insert("../../etc/cron.d/evil".to_string(), "malicious".to_string());
            m
        },
    };

    let payload_json = serde_json::to_string_pretty(&payload).unwrap();
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(payload_json.as_bytes());
    let crc32 = hasher.finalize();

    let bundle = agent_2pc::bundle::TransactionBundle {
        magic: "A2PC_BUNDLE_V1".to_string(),
        crc32,
        payload_json,
    };

    let bundle_file = tmp.path().join("evil.a2pc");
    fs::write(&bundle_file, serde_json::to_string_pretty(&bundle).unwrap()).unwrap();

    let res = BundleManager::import_bundle(&mut coord, &bundle_file);
    assert!(res.is_err());
    let err_msg = res.err().unwrap().to_string();
    assert!(err_msg.contains("Path traversal detected"), "Expected traversal error, got: {}", err_msg);
}

#[test]
fn test_fs_adapter_directory_and_missing_parent_rejection() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");
    let mut coord = Coordinator::new(&tx_dir).unwrap();
    let tid = coord.begin(Some(10000)).unwrap();

    // 1. Target is a directory
    let dir_target = tmp.path().join("a_directory");
    fs::create_dir_all(&dir_target).unwrap();

    let mut a1 = FsAdapter::new("fs_dir");
    let vote1 = coord
        .prepare_participant(
            &tid,
            &mut a1,
            ResourceIntent::FsWrite {
                path: dir_target.clone(),
                content: "fail".into(),
                create_dirs: false,
            },
        )
        .unwrap();
    assert!(matches!(vote1, ParticipantVote::Aborted { .. }));

    // 2. Parent directory missing and create_dirs = false
    let tid2 = coord.begin(Some(10000)).unwrap();
    let missing_parent_target = tmp.path().join("non_existent_subdir").join("file.txt");
    let mut a2 = FsAdapter::new("fs_noparent");
    let vote2 = coord
        .prepare_participant(
            &tid2,
            &mut a2,
            ResourceIntent::FsWrite {
                path: missing_parent_target,
                content: "fail".into(),
                create_dirs: false,
            },
        )
        .unwrap();
    assert!(matches!(vote2, ParticipantVote::Aborted { .. }));
}

#[test]
fn test_reconstruct_adapters_extended_coverage() {
    let pids = vec![
        ParticipantId::new("fs_file1"),
        ParticipantId::new("cmd_echo"),
        ParticipantId::new("gov_run"),
        ParticipantId::new("vm_container"),
        ParticipantId::new("wasi_plugin"),
        ParticipantId::new("sandbox_jail"),
        ParticipantId::new("git_branch"),
        ParticipantId::new("custom_api"),
    ];

    let adapters = reconstruct_adapters(&pids);
    assert_eq!(adapters.len(), 8);
    assert_eq!(adapters[0].id().0, "fs_file1");
    assert_eq!(adapters[1].id().0, "cmd_echo");
    assert_eq!(adapters[2].id().0, "gov_run");
    assert_eq!(adapters[3].id().0, "vm_container");
    assert_eq!(adapters[4].id().0, "wasi_plugin");
    assert_eq!(adapters[5].id().0, "sandbox_jail");
    assert_eq!(adapters[6].id().0, "git_branch");
    assert_eq!(adapters[7].id().0, "custom_api");
}
