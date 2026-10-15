//! Integration tests for Zero-Dependency UDS Daemon & IPC Protocol.

use agent_2pc::daemon::Daemon;
use agent_2pc::ipc::IpcClient;
use serde_json::json;
use std::fs;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

#[test]
fn test_daemon_lifecycle_and_ping() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");
    let daemon = Arc::new(Daemon::new(&tx_dir).unwrap());

    let d_clone = Arc::clone(&daemon);
    let handle = thread::spawn(move || {
        let _ = d_clone.run();
    });

    let sock_path = daemon.socket_path();

    // Wait for daemon to become ready
    let mut ready = false;
    for _ in 0..50 {
        if IpcClient::is_alive(sock_path) {
            ready = true;
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(ready, "Daemon must become ready within 1 second");

    // Connect client and send ping
    let mut client = IpcClient::try_connect(sock_path).unwrap();
    let ping_resp = client.call("ping", json!({})).unwrap();
    assert_eq!(ping_resp.get("status").unwrap().as_str().unwrap(), "ok");

    // Stop daemon
    let stop_resp = client.call("stop", json!({})).unwrap();
    assert_eq!(stop_resp.get("status").unwrap().as_str().unwrap(), "stopping");

    handle.join().unwrap();
    assert!(!sock_path.exists(), "Socket file must be cleaned up after daemon shutdown");
}

#[test]
fn test_daemon_transaction_workflow_over_uds() {
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

    // 1. Begin transaction
    let begin_resp = client.call("begin", json!({ "timeout_ms": 15000 })).unwrap();
    let tid = begin_resp.get("tid").unwrap().as_str().unwrap();

    // 2. Stage write (permanent file)
    let file1 = tmp.path().join("daemon_file1.txt");
    let stage1_resp = client
        .call(
            "stage_write",
            json!({
                "tid": tid,
                "path": file1.to_str().unwrap(),
                "content": "daemon permanent content",
                "create_dirs": true
            }),
        )
        .unwrap();
    assert_eq!(stage1_resp.get("vote").unwrap().as_str().unwrap(), "PREPARED");

    // 3. Mark Savepoint
    let sp_resp = client
        .call("savepoint", json!({ "tid": tid, "name": "sp_stable" }))
        .unwrap();
    assert_eq!(sp_resp.get("name").unwrap().as_str().unwrap(), "sp_stable");

    // 4. Stage speculative write (should be unwound)
    let file2 = tmp.path().join("daemon_file2.txt");
    let stage2_resp = client
        .call(
            "stage_write",
            json!({
                "tid": tid,
                "path": file2.to_str().unwrap(),
                "content": "daemon speculative content",
                "create_dirs": true
            }),
        )
        .unwrap();
    assert_eq!(stage2_resp.get("vote").unwrap().as_str().unwrap(), "PREPARED");

    // 5. Rollback to Savepoint
    let rollback_resp = client
        .call("rollback_to", json!({ "tid": tid, "name": "sp_stable" }))
        .unwrap();
    assert_eq!(
        rollback_resp
            .get("unwound_participants")
            .unwrap()
            .as_u64()
            .unwrap(),
        1
    );

    // 6. Commit transaction
    let commit_resp = client.call("commit", json!({ "tid": tid })).unwrap();
    assert_eq!(commit_resp.get("decision").unwrap().as_str().unwrap(), "COMMITTED");

    // Verify disk effects
    assert_eq!(fs::read_to_string(&file1).unwrap(), "daemon permanent content");
    assert!(!file2.exists(), "Speculative file must not exist on disk");

    // Stop daemon
    let _ = client.call("stop", json!({}));
    handle.join().unwrap();
}

#[test]
fn test_daemon_concurrent_clients() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");
    let daemon = Arc::new(Daemon::new(&tx_dir).unwrap());

    let d_clone = Arc::clone(&daemon);
    let handle = thread::spawn(move || {
        let _ = d_clone.run();
    });

    let sock_path = daemon.socket_path().to_path_buf();
    for _ in 0..50 {
        if IpcClient::is_alive(&sock_path) {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }

    let mut thread_handles = Vec::new();
    let tmp_path = tmp.path().to_path_buf();

    for i in 0..5 {
        let sock = sock_path.clone();
        let target_file = tmp_path.join(format!("concurrent_{}.txt", i));

        let th = thread::spawn(move || {
            let mut client = IpcClient::try_connect(&sock).unwrap();
            let begin_resp = client.call("begin", json!({ "timeout_ms": 10000 })).unwrap();
            let tid = begin_resp.get("tid").unwrap().as_str().unwrap();

            let stage_resp = client
                .call(
                    "stage_write",
                    json!({
                        "tid": tid,
                        "path": target_file.to_str().unwrap(),
                        "content": format!("Concurrent worker {}", i),
                        "create_dirs": true
                    }),
                )
                .unwrap();
            assert_eq!(stage_resp.get("vote").unwrap().as_str().unwrap(), "PREPARED");

            let commit_resp = client.call("commit", json!({ "tid": tid })).unwrap();
            assert_eq!(commit_resp.get("decision").unwrap().as_str().unwrap(), "COMMITTED");

            assert_eq!(
                fs::read_to_string(&target_file).unwrap(),
                format!("Concurrent worker {}", i)
            );
        });

        thread_handles.push(th);
    }

    for th in thread_handles {
        th.join().unwrap();
    }

    // Connect one more client and stop daemon
    let mut client = IpcClient::try_connect(&sock_path).unwrap();
    let _ = client.call("stop", json!({}));
    handle.join().unwrap();
}
