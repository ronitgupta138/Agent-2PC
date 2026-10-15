//! Integration tests for WASI runner and Expanded MCP Tool Surface.

use agent_2pc::adapters::ResourceManager;
use agent_2pc::coordinator::Coordinator;
use agent_2pc::domain::{ResourceIntent, TxState};
use agent_2pc::mcp::McpServer;
use agent_2pc::wasi::{WasiAdapter, WasiConfig};
use serde_json::json;
use std::fs;
use tempfile::tempdir;

#[test]
fn test_wasi_adapter_execution_and_rollback() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");
    let mut coord = Coordinator::new(&tx_dir).unwrap();
    let tid = coord.begin(Some(10000)).unwrap();

    let mut adapter = WasiAdapter::new("wasi_plugin_1", Some(WasiConfig {
        max_memory_mb: 64,
        timeout_ms: 5000,
        allow_filesystem_writes: true,
    }));

    let intent = ResourceIntent::Command {
        command: "wasm_eval('fib(20)')".into(),
        working_dir: tmp.path().to_path_buf(),
        env: std::collections::HashMap::new(),
    };

    let vote = coord.prepare_participant(&tid, &mut adapter, intent).unwrap();
    assert!(matches!(vote, agent_2pc::domain::ParticipantVote::Prepared(_)));

    let tx = coord.get_status(&tid).unwrap();
    assert_eq!(tx.participants.len(), 1);
    assert_eq!(tx.state, TxState::Prepared);

    let mut resources: Vec<Box<dyn ResourceManager>> = vec![Box::new(adapter)];
    coord.commit(&tid, &mut resources[..]).unwrap();
    assert_eq!(coord.get_status(&tid).unwrap().state, TxState::Committed);
}

#[test]
fn test_expanded_mcp_tools_surface_and_exec() {
    let tmp = tempdir().unwrap();
    let tx_dir = tmp.path().join(".agent-tx");
    let mcp = McpServer::new(&tx_dir);

    // 1. Initialize
    let init_resp = mcp.call_tool("agent_2pc_begin", &json!({ "timeout_ms": 20000 })).unwrap();
    let tid = init_resp.get("tid").unwrap().as_str().unwrap();

    // 2. Call agent_2pc_exec
    let script_file = tmp.path().join("mcp_exec.txt");
    let exec_resp = mcp.call_tool("agent_2pc_exec", &json!({
        "tid": tid,
        "command": format!("echo 'staged from mcp' > {}", script_file.display()),
        "dir": tmp.path().to_str().unwrap()
    })).unwrap();

    assert_eq!(exec_resp.get("exit_code").unwrap().as_i64().unwrap(), 0);

    // 3. Call agent_2pc_daemon_status
    let status_resp = mcp.call_tool("agent_2pc_daemon_status", &json!({})).unwrap();
    assert!(status_resp.get("socket").is_some());

    // 4. Commit via MCP
    let commit_resp = mcp.call_tool("agent_2pc_commit", &json!({ "tid": tid })).unwrap();
    assert_eq!(commit_resp.get("decision").unwrap().as_str().unwrap(), "COMMITTED");

    assert_eq!(fs::read_to_string(&script_file).unwrap().trim(), "staged from mcp");
}
