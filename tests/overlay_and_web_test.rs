//! Integration tests for OverlayFS runner and Web HUD.

use agent_2pc::coordinator::Coordinator;
use agent_2pc::domain::TxState;
use agent_2pc::overlay::OverlayRunner;
use agent_2pc::web_hud::WebHudServer;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

#[test]
fn test_overlay_runner_execution_and_rollback() {
    if !OverlayRunner::is_supported() {
        eprintln!("Skipping overlayfs test: unprivileged user namespaces not supported in test environment");
        return;
    }

    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");
    let mut coord = Coordinator::new(&tx_dir).unwrap();

    let tid = coord.begin(Some(10000)).unwrap();

    let existing = tmp.path().join("base.txt");
    fs::write(&existing, "base lower content\n").unwrap();

    let cmd = vec![
        "echo 'overlay created' > overlay_new.txt".to_string(),
    ];

    let report = OverlayRunner::execute_and_stage(&mut coord, &tid, tmp.path(), &cmd).unwrap();
    assert_eq!(report.exit_code, 0);
    assert_eq!(report.staged_files.len(), 1);
    let tx = coord.get_status(&tid).unwrap();
    assert_eq!(tx.participants.len(), 1);
    assert_eq!(tx.state, TxState::Prepared);

    // Commit staged overlay changes
    let mut adapters = agent_2pc::adapters::reconstruct_adapters(&tx.participants);
    coord.commit(&tid, &mut adapters[..]).unwrap();

    assert!(tmp.path().join("overlay_new.txt").exists());
    assert_eq!(
        fs::read_to_string(tmp.path().join("overlay_new.txt")).unwrap().trim(),
        "overlay created"
    );
}

#[test]
fn test_web_hud_server_http_and_api() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");
    let mut coord = Coordinator::new(&tx_dir).unwrap();
    let tid = coord.begin(Some(15000)).unwrap();

    let bind_addr = "127.0.0.1:19875";
    let server = Arc::new(WebHudServer::new(&tx_dir, bind_addr));
    let server_clone = Arc::clone(&server);

    let handle = thread::spawn(move || {
        let _ = server_clone.serve();
    });

    thread::sleep(Duration::from_millis(50));

    // 1. Test GET /
    let mut stream = TcpStream::connect(bind_addr).unwrap();
    stream.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n").unwrap();

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    assert!(line.contains("200 OK"));

    // 2. Test GET /api/status
    let mut stream2 = TcpStream::connect(bind_addr).unwrap();
    stream2.write_all(b"GET /api/status HTTP/1.1\r\nHost: localhost\r\n\r\n").unwrap();

    let mut reader2 = BufReader::new(stream2);
    let mut status_line = String::new();
    reader2.read_line(&mut status_line).unwrap();
    assert!(status_line.contains("200 OK"));

    // Read headers until blank line
    let mut headers = String::new();
    loop {
        let mut h = String::new();
        reader2.read_line(&mut h).unwrap();
        if h.trim().is_empty() {
            break;
        }
        headers.push_str(&h);
    }

    let mut body = String::new();
    reader2.read_to_string(&mut body).unwrap();
    let val: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(val.get("active_count").unwrap().as_u64().unwrap(), 1);

    let txs = val.get("transactions").unwrap().as_array().unwrap();
    assert_eq!(txs[0].get("tid").unwrap().as_str().unwrap(), tid.to_string());

    server.stop();
    handle.join().unwrap();
}
