use agent_2pc::adapters::FsAdapter;
use agent_2pc::coordinator::Coordinator;
use agent_2pc::daemon::Daemon;
use agent_2pc::domain::ResourceIntent;
use agent_2pc::git::GitIntegrator;
use agent_2pc::ipc::IpcClient;
use serde_json::json;
use std::fs;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

#[test]
fn test_git_hook_installation_and_clean_check() {
    let tmp = tempdir().unwrap();
    let git_root = tmp.path().join("repo");
    fs::create_dir_all(&git_root).unwrap();

    let tx_dir = git_root.join(".agent-tx");
    fs::create_dir_all(&tx_dir).unwrap();

    // 1. Install hook
    let report = GitIntegrator::install_hook(&git_root, &tx_dir).unwrap();
    assert!(report.installed);
    assert!(report.hook_path.exists());

    let content = fs::read_to_string(&report.hook_path).unwrap();
    assert!(content.contains("agent-2pc hook check"));

    // 2. Check clean repository
    let check = GitIntegrator::check_invariants(&tx_dir).unwrap();
    assert!(check.clean);
    assert!(check.in_flight_tids.is_empty());
}

#[test]
fn test_git_hook_detects_in_flight_transactions() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");
    let mut coord = Coordinator::new(&tx_dir).unwrap();

    let tid = coord.begin(Some(30000)).unwrap();
    let target = tmp.path().join("in_flight.txt");
    let mut adapter = FsAdapter::new("p_hook");
    let intent = ResourceIntent::FsWrite {
        path: target,
        content: "test content\n".to_string(),
        create_dirs: false,
    };
    let _ = coord.prepare_participant(&tid, &mut adapter, intent).unwrap();

    // Repository now has an active prepared transaction
    let check = GitIntegrator::check_invariants(&tx_dir).unwrap();
    assert!(!check.clean);
    assert_eq!(check.in_flight_tids.len(), 1);
    assert_eq!(check.in_flight_tids[0], tid);
}

#[test]
fn test_daemon_ttl_reaper_aborts_expired_transactions() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");

    let daemon = Arc::new(Daemon::new(&tx_dir).unwrap());
    let d_clone = Arc::clone(&daemon);
    let handle = thread::spawn(move || {
        let _ = d_clone.run();
    });

    let sock_path = daemon.socket_path();
    for _ in 0..50 {
        if IpcClient::is_alive(sock_path) {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }

    let mut client = IpcClient::try_connect(sock_path).unwrap();

    // Begin a short-lived transaction (50ms timeout)
    let begin_res = client.call("begin", json!({ "timeout_ms": 50 })).unwrap();
    let tid = begin_res.get("tid").unwrap().as_str().unwrap();

    let target = tmp.path().join("reaper_target.txt");
    let _ = client.call("stage_write", json!({
        "tid": tid,
        "path": target.to_str().unwrap(),
        "content": "to be reaped\n",
        "create_dirs": false
    })).unwrap();

    // Wait 400ms for reaper thread to detect TTL expiry and sweep
    thread::sleep(Duration::from_millis(400));

    // Inspect status: transaction should be transitioned to Aborted
    let status_res = client.call("status", json!({ "tid": tid })).unwrap();
    let state = status_res.get("state").unwrap().as_str().unwrap();
    assert_eq!(state, "ABORTED");

    let _ = client.call("stop", json!({}));
    handle.join().unwrap();
}

#[test]
fn test_daemon_reap_ipc_command() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");

    let daemon = Arc::new(Daemon::new(&tx_dir).unwrap());
    let d_clone = Arc::clone(&daemon);
    let handle = thread::spawn(move || {
        let _ = d_clone.run();
    });

    let sock_path = daemon.socket_path();
    for _ in 0..50 {
        if IpcClient::is_alive(sock_path) {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }

    let mut client = IpcClient::try_connect(sock_path).unwrap();

    // Begin transaction with 10ms timeout
    let _ = client.call("begin", json!({ "timeout_ms": 10 })).unwrap();
    thread::sleep(Duration::from_millis(30));

    // Manually trigger reap
    let reap_res = client.call("reap", json!({})).unwrap();
    let reaped_count = reap_res.get("reaped_count").unwrap().as_u64().unwrap();
    assert!(reaped_count >= 1);

    let _ = client.call("stop", json!({}));
    handle.join().unwrap();
}
