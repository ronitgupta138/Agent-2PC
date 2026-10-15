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
    assert_eq!(init_res["result"]["serverInfo"]["version"], env!("CARGO_PKG_VERSION"));

    let tools_res: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
    let tools = tools_res["result"]["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 15);

    // Verify all 4 required M8ven / OpenAI hints exist at root and in annotations object
    for t in tools {
        // Root-level hints
        assert!(t.get("readOnlyHint").unwrap().is_boolean(), "Tool {} missing root readOnlyHint", t["name"]);
        assert!(t.get("destructiveHint").unwrap().is_boolean(), "Tool {} missing root destructiveHint", t["name"]);
        assert!(t.get("idempotentHint").unwrap().is_boolean(), "Tool {} missing root idempotentHint", t["name"]);
        assert!(t.get("openWorldHint").unwrap().is_boolean(), "Tool {} missing root openWorldHint", t["name"]);

        // Standard annotations object hints
        let ann = t.get("annotations").unwrap_or_else(|| panic!("Tool {} missing annotations object", t["name"])).as_object().unwrap();
        assert!(ann.get("readOnlyHint").unwrap().is_boolean());
        assert!(ann.get("destructiveHint").unwrap().is_boolean());
        assert!(ann.get("idempotentHint").unwrap().is_boolean());
        assert!(ann.get("openWorldHint").unwrap().is_boolean());
    }
}

#[test]
fn test_mcp_full_tool_surface_execution() {
    let tmp = tempdir().unwrap();
    let mcp = McpServer::new(tmp.path().join(".agent-tx"));
    let target_file = tmp.path().join("mcp_full_test.txt");
    fs::write(&target_file, "original content\n").unwrap();

    // 1. agent_2pc_begin
    let begin_res = mcp.call_tool("agent_2pc_begin", &json!({ "timeout_ms": 30000 })).unwrap();
    let tid = begin_res.get("tid").unwrap().as_str().unwrap();

    // 2. agent_2pc_record_read
    let read_res = mcp.call_tool("agent_2pc_record_read", &json!({
        "tid": tid,
        "path": target_file.to_str().unwrap()
    })).unwrap();
    assert!(read_res.get("hash").is_some());

    // 3. agent_2pc_stage_write
    let write_res = mcp.call_tool("agent_2pc_stage_write", &json!({
        "tid": tid,
        "path": target_file.to_str().unwrap(),
        "content": "staged overwrite\n",
        "create_dirs": true
    })).unwrap();
    assert_eq!(write_res.get("vote").unwrap().as_str().unwrap(), "PREPARED");

    // 4. agent_2pc_savepoint
    let sp_res = mcp.call_tool("agent_2pc_savepoint", &json!({
        "tid": tid,
        "name": "sp1"
    })).unwrap();
    assert_eq!(sp_res.get("name").unwrap().as_str().unwrap(), "sp1");

    // 5. agent_2pc_stage_patch
    let patch_file = tmp.path().join("patch_target.txt");
    fs::write(&patch_file, "line1\nline2\n").unwrap();
    let patch_res = mcp.call_tool("agent_2pc_stage_patch", &json!({
        "tid": tid,
        "path": patch_file.to_str().unwrap(),
        "patch": "line3\n"
    })).unwrap();
    assert_eq!(patch_res.get("vote").unwrap().as_str().unwrap(), "PREPARED");

    // 6. agent_2pc_stage_cmd
    let cmd_res = mcp.call_tool("agent_2pc_stage_cmd", &json!({
        "tid": tid,
        "command": "true"
    })).unwrap();
    assert_eq!(cmd_res.get("vote").unwrap().as_str().unwrap(), "PREPARED");

    // 7. agent_2pc_stage_api
    let api_res = mcp.call_tool("agent_2pc_stage_api", &json!({
        "tid": tid,
        "endpoint": "https://api.example.com/v1/mutate"
    })).unwrap();
    assert_eq!(api_res.get("vote").unwrap().as_str().unwrap(), "PREPARED");

    // 8. agent_2pc_rollback_to (unwinds patch, cmd, and api, keeps write at sp1)
    let unwound_res = mcp.call_tool("agent_2pc_rollback_to", &json!({
        "tid": tid,
        "name": "sp1"
    })).unwrap();
    assert_eq!(unwound_res.get("unwound_participants").unwrap().as_u64().unwrap(), 3);

    // 9. agent_2pc_status
    let status_res = mcp.call_tool("agent_2pc_status", &json!({ "tid": tid })).unwrap();
    assert_eq!(status_res.get("tid").unwrap().as_str().unwrap(), tid);

    // 10. agent_2pc_daemon_status
    let daemon_res = mcp.call_tool("agent_2pc_daemon_status", &json!({})).unwrap();
    assert!(daemon_res.get("socket").is_some());

    // 11. agent_2pc_commit
    let commit_res = mcp.call_tool("agent_2pc_commit", &json!({ "tid": tid })).unwrap();
    assert_eq!(commit_res.get("decision").unwrap().as_str().unwrap(), "COMMITTED");
    assert_eq!(fs::read_to_string(&target_file).unwrap(), "staged overwrite\n");

    // 12. agent_2pc_compact
    let compact_res = mcp.call_tool("agent_2pc_compact", &json!({})).unwrap();
    assert!(compact_res.get("compacted_records").is_some());

    // 13. agent_2pc_abort flow
    let abort_tid_res = mcp.call_tool("agent_2pc_begin", &json!({})).unwrap();
    let abort_tid = abort_tid_res.get("tid").unwrap().as_str().unwrap();
    let abort_res = mcp.call_tool("agent_2pc_abort", &json!({
        "tid": abort_tid,
        "reason": "Test abort verification"
    })).unwrap();
    assert_eq!(abort_res.get("decision").unwrap().as_str().unwrap(), "ABORTED");

    // 14. agent_2pc_exec
    let exec_tid_res = mcp.call_tool("agent_2pc_begin", &json!({})).unwrap();
    let exec_tid = exec_tid_res.get("tid").unwrap().as_str().unwrap();
    let exec_out_file = tmp.path().join("exec_out.txt");
    let exec_res = mcp.call_tool("agent_2pc_exec", &json!({
        "tid": exec_tid,
        "command": format!("echo 'exec tool verified' > {}", exec_out_file.display()),
        "dir": tmp.path().to_str().unwrap()
    })).unwrap();
    assert_eq!(exec_res.get("exit_code").unwrap().as_i64().unwrap(), 0);

    // 15. agent_2pc_overlay_exec
    if agent_2pc::overlay::OverlayRunner::is_supported() {
        let ovl_tid_res = mcp.call_tool("agent_2pc_begin", &json!({})).unwrap();
        let ovl_tid = ovl_tid_res.get("tid").unwrap().as_str().unwrap();
        let _ = mcp.call_tool("agent_2pc_overlay_exec", &json!({
            "tid": ovl_tid,
            "command": "true",
            "dir": tmp.path().to_str().unwrap()
        }));
    }
}
