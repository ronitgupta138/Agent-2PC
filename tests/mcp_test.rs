use agent_2pc::mcp::McpServer;
use serde_json::json;
use std::fs;
use std::io::Cursor;
use tempfile::tempdir;

#[test]
fn test_mcp_initialize_and_tools_list() {
    let tmp = tempdir().unwrap();
    let mcp = McpServer::new(tmp.path().join(".agent-tx"));

    let input = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{}}\n\
                 {\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\",\"params\":{}}\n";

    let mut output = Vec::new();
    mcp.run_loop(Cursor::new(input), &mut output).unwrap();

    let output_str = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = output_str.trim().split('\n').collect();
    assert_eq!(lines.len(), 2);

    let init_res: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(init_res["result"]["serverInfo"]["name"], "agent-2pc");
    assert_eq!(init_res["result"]["serverInfo"]["version"], "0.2.0");

    let tools_res: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
    let tools = tools_res["result"]["tools"].as_array().unwrap();
    assert!(tools.len() >= 6);
}

#[test]
fn test_mcp_begin_stage_commit_flow() {
    let tmp = tempdir().unwrap();
    let mcp = McpServer::new(tmp.path().join(".agent-tx"));
    let target_file = tmp.path().join("mcp_output.txt");

    // 1. Begin
    let begin_req = json!({
        "jsonrpc": "2.0",
        "id": 10,
        "method": "tools/call",
        "params": {
            "name": "agent_2pc_begin",
            "arguments": { "timeout_ms": 15000 }
        }
    });

    let mut begin_out = Vec::new();
    mcp.run_loop(Cursor::new(serde_json::to_string(&begin_req).unwrap() + "\n"), &mut begin_out).unwrap();
    let begin_res: serde_json::Value = serde_json::from_slice(&begin_out).unwrap();
    let begin_text = begin_res["result"]["content"][0]["text"].as_str().unwrap();
    let begin_payload: serde_json::Value = serde_json::from_str(begin_text).unwrap();
    let tid = begin_payload["tid"].as_str().unwrap();

    // 2. Stage write
    let stage_req = json!({
        "jsonrpc": "2.0",
        "id": 11,
        "method": "tools/call",
        "params": {
            "name": "agent_2pc_stage_write",
            "arguments": {
                "tid": tid,
                "path": target_file.to_string_lossy(),
                "content": "Content written via native Rust MCP server!"
            }
        }
    });

    let mut stage_out = Vec::new();
    mcp.run_loop(Cursor::new(serde_json::to_string(&stage_req).unwrap() + "\n"), &mut stage_out).unwrap();

    // Target file must NOT exist before commit
    assert!(!target_file.exists());

    // 3. Commit
    let commit_req = json!({
        "jsonrpc": "2.0",
        "id": 12,
        "method": "tools/call",
        "params": {
            "name": "agent_2pc_commit",
            "arguments": { "tid": tid }
        }
    });

    let mut commit_out = Vec::new();
    mcp.run_loop(Cursor::new(serde_json::to_string(&commit_req).unwrap() + "\n"), &mut commit_out).unwrap();

    // Target file MUST exist after commit
    assert!(target_file.exists());
    assert_eq!(
        fs::read_to_string(&target_file).unwrap(),
        "Content written via native Rust MCP server!"
    );
}
