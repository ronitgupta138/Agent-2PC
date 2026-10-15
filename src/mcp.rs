//! Native embedded Model Context Protocol (MCP) JSON-RPC 2.0 stdio server.

use crate::adapters::{ApiAdapter, CommandAdapter, FsAdapter, ResourceManager};
use crate::coordinator::Coordinator;
use crate::domain::{ParticipantVote, ResourceIntent, TransactionId, TxError};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};

/// JSON-RPC 2.0 Request framing
#[derive(Debug, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub id: Option<Value>,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

/// JSON-RPC 2.0 Response framing
#[derive(Debug, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: &'static str,
    pub id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<Value>,
}

/// Native MCP Server runner driving coordinator primitives over stdio.
pub struct McpServer {
    tx_dir: PathBuf,
}

impl McpServer {
    pub fn new(tx_dir: impl AsRef<Path>) -> Self {
        Self {
            tx_dir: tx_dir.as_ref().to_path_buf(),
        }
    }

    /// Run JSON-RPC event loop over provided reader and writer streams.
    pub fn run_loop<R: BufRead, W: Write>(&self, reader: R, mut writer: W) -> Result<(), TxError> {
        for line_res in reader.lines() {
            let line = line_res?;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            if let Ok(req) = serde_json::from_str::<JsonRpcRequest>(trimmed) {
                let id = req.id.clone().unwrap_or(Value::Null);

                // Notifications (no ID) do not expect a response
                let is_notification = req.id.is_none();

                let resp_val = self.handle_request(&req);

                if !is_notification {
                    let resp = match resp_val {
                        Ok(res) => JsonRpcResponse {
                            jsonrpc: "2.0",
                            id,
                            result: Some(res),
                            error: None,
                        },
                        Err(err) => JsonRpcResponse {
                            jsonrpc: "2.0",
                            id,
                            result: None,
                            error: Some(json!({
                                "code": -32000,
                                "message": err.to_string()
                            })),
                        },
                    };

                    let out = serde_json::to_string(&resp)?;
                    writer.write_all(out.as_bytes())?;
                    writer.write_all(b"\n")?;
                    writer.flush()?;
                }
            }
        }
        Ok(())
    }

    /// Dispatch incoming method calls.
    pub fn handle_request(&self, req: &JsonRpcRequest) -> Result<Value, TxError> {
        match req.method.as_str() {
            "initialize" => Ok(json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "tools": {}
                },
                "serverInfo": {
                    "name": "agent-2pc",
                    "version": env!("CARGO_PKG_VERSION")
                }
            })),

            "notifications/initialized" => Ok(json!({})),

            "ping" => Ok(json!({})),

            "tools/list" => Ok(json!({
                "tools": self.list_tools()
            })),

            "tools/call" => {
                let name = req.params.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let args = req.params.get("arguments").cloned().unwrap_or(json!({}));
                let tool_output = self.call_tool(name, &args)?;
                Ok(json!({
                    "content": [
                        {
                            "type": "text",
                            "text": serde_json::to_string_pretty(&tool_output)?
                        }
                    ]
                }))
            }

            other => Err(TxError::Other(format!("Unsupported method: {}", other))),
        }
    }

    /// List tool schemas exposed via MCP.
    fn list_tools(&self) -> Vec<Value> {
        vec![
            json!({
                "name": "agent_2pc_begin",
                "description": "Start a new 2PC transaction session and obtain a unique Transaction ID (TID).",
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": false,
                "openWorldHint": false,
                "annotations": {
                    "readOnlyHint": false,
                    "destructiveHint": false,
                    "idempotentHint": false,
                    "openWorldHint": false
                },
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "timeout_ms": { "type": "integer", "description": "Transaction timeout in ms (default: 30000)" }
                    }
                }
            }),
            json!({
                "name": "agent_2pc_stage_write",
                "description": "Phase 1: Stage a file write into isolated scratch space with automatic .bak backup.",
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false,
                "annotations": {
                    "readOnlyHint": false,
                    "destructiveHint": false,
                    "idempotentHint": true,
                    "openWorldHint": false
                },
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "tid": { "type": "string", "description": "Active Transaction ID" },
                        "path": { "type": "string", "description": "Destination file path" },
                        "content": { "type": "string", "description": "File content to stage" },
                        "create_dirs": { "type": "boolean", "description": "Auto-create parent directories" }
                    },
                    "required": ["tid", "path", "content"]
                }
            }),
            json!({
                "name": "agent_2pc_stage_patch",
                "description": "Phase 1: Stage a unified diff patch into isolated scratch memory.",
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false,
                "annotations": {
                    "readOnlyHint": false,
                    "destructiveHint": false,
                    "idempotentHint": true,
                    "openWorldHint": false
                },
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "tid": { "type": "string", "description": "Active Transaction ID" },
                        "path": { "type": "string", "description": "Destination file path" },
                        "patch": { "type": "string", "description": "Patch diff content" }
                    },
                    "required": ["tid", "path", "patch"]
                }
            }),
            json!({
                "name": "agent_2pc_stage_diff",
                "description": "Phase 1: Stage a targeted unique substring find-and-replace in scratch memory.",
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false,
                "annotations": {
                    "readOnlyHint": false,
                    "destructiveHint": false,
                    "idempotentHint": true,
                    "openWorldHint": false
                },
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "tid": { "type": "string", "description": "Active Transaction ID" },
                        "path": { "type": "string", "description": "Destination file path" },
                        "old_string": { "type": "string", "description": "Exact unique string to find and replace" },
                        "new_string": { "type": "string", "description": "Replacement string" }
                    },
                    "required": ["tid", "path", "old_string", "new_string"]
                }
            }),
            json!({
                "name": "agent_2pc_stage_cmd",
                "description": "Phase 1: Run and verify a command in an isolated scratch sandbox before commit.",
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": false,
                "openWorldHint": true,
                "annotations": {
                    "readOnlyHint": false,
                    "destructiveHint": false,
                    "idempotentHint": false,
                    "openWorldHint": true
                },
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "tid": { "type": "string", "description": "Active Transaction ID" },
                        "command": { "type": "string", "description": "Shell command to test" }
                    },
                    "required": ["tid", "command"]
                }
            }),
            json!({
                "name": "agent_2pc_stage_api",
                "description": "Phase 1: Reserve an external API lease with idempotency key and compensation hook.",
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": true,
                "annotations": {
                    "readOnlyHint": false,
                    "destructiveHint": false,
                    "idempotentHint": true,
                    "openWorldHint": true
                },
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "tid": { "type": "string", "description": "Active Transaction ID" },
                        "endpoint": { "type": "string", "description": "Target endpoint URL" },
                        "method": { "type": "string", "description": "HTTP method (POST, etc.)" },
                        "idempotency_key": { "type": "string", "description": "Unique lease key" },
                        "compensation_endpoint": { "type": "string", "description": "Rollback compensation URL" }
                    },
                    "required": ["tid", "endpoint"]
                }
            }),
            json!({
                "name": "agent_2pc_commit",
                "description": "Phase 2: Globally and atomically commit all staged operations.",
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": true,
                "annotations": {
                    "readOnlyHint": false,
                    "destructiveHint": false,
                    "idempotentHint": true,
                    "openWorldHint": true
                },
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "tid": { "type": "string", "description": "Active Transaction ID" }
                    },
                    "required": ["tid"]
                }
            }),
            json!({
                "name": "agent_2pc_abort",
                "description": "Phase 2: Deterministically roll back all staged changes and restore backups.",
                "readOnlyHint": false,
                "destructiveHint": true,
                "idempotentHint": true,
                "openWorldHint": true,
                "annotations": {
                    "readOnlyHint": false,
                    "destructiveHint": true,
                    "idempotentHint": true,
                    "openWorldHint": true
                },
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "tid": { "type": "string", "description": "Active Transaction ID" },
                        "reason": { "type": "string", "description": "Reason for aborting" }
                    },
                    "required": ["tid"]
                }
            }),
            json!({
                "name": "agent_2pc_status",
                "description": "Inspect status of an active transaction or list all in-flight transactions.",
                "readOnlyHint": true,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false,
                "annotations": {
                    "readOnlyHint": true,
                    "destructiveHint": false,
                    "idempotentHint": true,
                    "openWorldHint": false
                },
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "tid": { "type": "string", "description": "Optional specific Transaction ID" }
                    }
                }
            }),
            json!({
                "name": "agent_2pc_record_read",
                "description": "Record a file in read-set for optimistic concurrency control (OCC). Fails prepare if modified concurrently.",
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false,
                "annotations": {
                    "readOnlyHint": false,
                    "destructiveHint": false,
                    "idempotentHint": true,
                    "openWorldHint": false
                },
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "tid": { "type": "string", "description": "Active Transaction ID" },
                        "path": { "type": "string", "description": "Path to file read" }
                    },
                    "required": ["tid", "path"]
                }
            }),
            json!({
                "name": "agent_2pc_compact",
                "description": "Compact WAL by purging finalized (committed/aborted) transactions while preserving in-flight state.",
                "readOnlyHint": false,
                "destructiveHint": true,
                "idempotentHint": true,
                "openWorldHint": false,
                "annotations": {
                    "readOnlyHint": false,
                    "destructiveHint": true,
                    "idempotentHint": true,
                    "openWorldHint": false
                },
                "inputSchema": {
                    "type": "object",
                    "properties": {}
                }
            }),
            json!({
                "name": "agent_2pc_savepoint",
                "description": "Create a transactional checkpoint (savepoint) within an active transaction for speculative execution.",
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": false,
                "openWorldHint": false,
                "annotations": {
                    "readOnlyHint": false,
                    "destructiveHint": false,
                    "idempotentHint": false,
                    "openWorldHint": false
                },
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "tid": { "type": "string", "description": "Active Transaction ID" },
                        "name": { "type": "string", "description": "Identifier name for the savepoint" }
                    },
                    "required": ["tid", "name"]
                }
            }),
            json!({
                "name": "agent_2pc_rollback_to",
                "description": "Roll back transaction changes staged after a named savepoint, restoring earlier verified state.",
                "readOnlyHint": false,
                "destructiveHint": true,
                "idempotentHint": true,
                "openWorldHint": false,
                "annotations": {
                    "readOnlyHint": false,
                    "destructiveHint": true,
                    "idempotentHint": true,
                    "openWorldHint": false
                },
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "tid": { "type": "string", "description": "Active Transaction ID" },
                        "name": { "type": "string", "description": "Name of the savepoint to restore" }
                    },
                    "required": ["tid", "name"]
                }
            }),
            json!({
                "name": "agent_2pc_exec",
                "description": "Execute a shell command and automatically stage any created or modified files into Phase 1.",
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": false,
                "openWorldHint": true,
                "annotations": {
                    "readOnlyHint": false,
                    "destructiveHint": false,
                    "idempotentHint": false,
                    "openWorldHint": true
                },
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "tid": { "type": "string", "description": "Active Transaction ID" },
                        "command": { "type": "string", "description": "Command string to execute" },
                        "dir": { "type": "string", "description": "Working directory (defaults to current dir)" }
                    },
                    "required": ["tid", "command"]
                }
            }),
            json!({
                "name": "agent_2pc_overlay_exec",
                "description": "Execute a command inside an ephemeral kernel OverlayFS sandbox with sub-50µs rollbacks.",
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": false,
                "openWorldHint": true,
                "annotations": {
                    "readOnlyHint": false,
                    "destructiveHint": false,
                    "idempotentHint": false,
                    "openWorldHint": true
                },
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "tid": { "type": "string", "description": "Active Transaction ID" },
                        "command": { "type": "string", "description": "Command string to execute" },
                        "dir": { "type": "string", "description": "Working directory (defaults to current dir)" }
                    },
                    "required": ["tid", "command"]
                }
            }),
            json!({
                "name": "agent_2pc_daemon_status",
                "description": "Check whether the background in-memory UDS daemon is active and responding.",
                "readOnlyHint": true,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false,
                "annotations": {
                    "readOnlyHint": true,
                    "destructiveHint": false,
                    "idempotentHint": true,
                    "openWorldHint": false
                },
                "inputSchema": {
                    "type": "object",
                    "properties": {}
                }
            }),
            json!({
                "name": "agent_2pc_diff",
                "description": "Inspect unified diff and stats for all staged file modifications in an active transaction before commit.",
                "readOnlyHint": true,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false,
                "annotations": {
                    "readOnlyHint": true,
                    "destructiveHint": false,
                    "idempotentHint": true,
                    "openWorldHint": false
                },
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "tid": { "type": "string", "description": "Active Transaction ID" },
                        "stat": { "type": "boolean", "description": "Return summary diffstat only" }
                    },
                    "required": ["tid"]
                }
            }),
            json!({
                "name": "agent_2pc_export",
                "description": "Export an active transaction and all its staged artifacts to a portable verified .a2pc bundle.",
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false,
                "annotations": {
                    "readOnlyHint": false,
                    "destructiveHint": false,
                    "idempotentHint": true,
                    "openWorldHint": false
                },
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "tid": { "type": "string", "description": "Active Transaction ID" },
                        "output": { "type": "string", "description": "Destination file path (.a2pc)" }
                    },
                    "required": ["tid"]
                }
            }),
            json!({
                "name": "agent_2pc_import",
                "description": "Import a verified transaction bundle (.a2pc) into this agent coordinator as a prepared transaction.",
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": false,
                "openWorldHint": false,
                "annotations": {
                    "readOnlyHint": false,
                    "destructiveHint": false,
                    "idempotentHint": false,
                    "openWorldHint": false
                },
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "file": { "type": "string", "description": "Bundle file path (.a2pc)" }
                    },
                    "required": ["file"]
                }
            }),
            json!({
                "name": "agent_2pc_hook_check",
                "description": "Verify repository git integrity: assert zero in-flight uncommitted 2PC transactions exist.",
                "readOnlyHint": true,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false,
                "annotations": {
                    "readOnlyHint": true,
                    "destructiveHint": false,
                    "idempotentHint": true,
                    "openWorldHint": false
                },
                "inputSchema": {
                    "type": "object",
                    "properties": {}
                }
            }),
            json!({
                "name": "agent_2pc_git_commit",
                "description": "Atomically commit active 2PC transaction and immediately create a Git commit with touched files.",
                "readOnlyHint": false,
                "destructiveHint": true,
                "idempotentHint": true,
                "openWorldHint": true,
                "annotations": {
                    "readOnlyHint": false,
                    "destructiveHint": true,
                    "idempotentHint": true,
                    "openWorldHint": true
                },
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "tid": { "type": "string", "description": "Active Transaction ID" },
                        "message": { "type": "string", "description": "Git commit message" }
                    },
                    "required": ["tid", "message"]
                }
            })
        ]
    }

    /// Execute a tool call.
    pub fn call_tool(&self, name: &str, args: &Value) -> Result<Value, TxError> {
        let sock_path = self.tx_dir.join("agent-2pc.sock");
        if let Ok(mut client) = crate::ipc::IpcClient::try_connect(&sock_path) {
            let method_name = match name {
                "agent_2pc_begin" => Some("begin"),
                "agent_2pc_stage_write" => Some("stage_write"),
                "agent_2pc_stage_patch" => Some("stage_patch"),
                "agent_2pc_stage_cmd" => Some("stage_cmd"),
                "agent_2pc_stage_api" => Some("stage_api"),
                "agent_2pc_commit" => Some("commit"),
                "agent_2pc_abort" => Some("abort"),
                "agent_2pc_status" => Some("status"),
                "agent_2pc_record_read" => Some("record_read"),
                "agent_2pc_compact" => Some("compact"),
                "agent_2pc_savepoint" => Some("savepoint"),
                "agent_2pc_rollback_to" => Some("rollback_to"),
                "agent_2pc_stage_diff" => Some("stage_replace"),
                _ => None,
            };

            if let Some(method) = method_name
                && let Ok(daemon_res) = client.call(method, args.clone()) {
                    return Ok(daemon_res);
            }
        }

        let mut coord = Coordinator::new(&self.tx_dir)?;

        match name {
            "agent_2pc_begin" => {
                let timeout_ms = args.get("timeout_ms").and_then(|v| v.as_u64());
                let tid = coord.begin(timeout_ms)?;
                Ok(json!({
                    "status": "ok",
                    "tid": tid.to_string(),
                    "timeout_ms": timeout_ms.unwrap_or(30000)
                }))
            }

            "agent_2pc_stage_write" => {
                let tid_str = args.get("tid").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: tid".into())
                })?;
                let path_str = args.get("path").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: path".into())
                })?;
                let content = args.get("content").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: content".into())
                })?;
                let create_dirs = args.get("create_dirs").and_then(|v| v.as_bool()).unwrap_or(true);

                let tid = TransactionId::parse(tid_str)?;
                let dest_path = PathBuf::from(path_str);
                let pid = format!("fs_write_{}", dest_path.file_name().unwrap_or_default().to_string_lossy());

                let mut adapter = FsAdapter::new(pid);
                let intent = ResourceIntent::FsWrite {
                    path: dest_path,
                    content: content.to_string(),
                    create_dirs,
                };

                let vote = coord.prepare_participant(&tid, &mut adapter, intent)?;
                match vote {
                    ParticipantVote::Prepared(receipt) => Ok(json!({
                        "status": "ok",
                        "vote": "PREPARED",
                        "receipt": receipt
                    })),
                    ParticipantVote::Aborted { reason } => Ok(json!({
                        "status": "error",
                        "vote": "ABORTED",
                        "reason": reason
                    })),
                }
            }

            "agent_2pc_stage_patch" => {
                let tid_str = args.get("tid").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: tid".into())
                })?;
                let path_str = args.get("path").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: path".into())
                })?;
                let patch = args.get("patch").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: patch".into())
                })?;

                let tid = TransactionId::parse(tid_str)?;
                let dest_path = PathBuf::from(path_str);
                let pid = format!("fs_patch_{}", dest_path.file_name().unwrap_or_default().to_string_lossy());

                let mut adapter = FsAdapter::new(pid);
                let intent = ResourceIntent::FsPatch {
                    path: dest_path,
                    patch: patch.to_string(),
                };

                let vote = coord.prepare_participant(&tid, &mut adapter, intent)?;
                match vote {
                    ParticipantVote::Prepared(receipt) => Ok(json!({
                        "status": "ok",
                        "vote": "PREPARED",
                        "receipt": receipt
                    })),
                    ParticipantVote::Aborted { reason } => Ok(json!({
                        "status": "error",
                        "vote": "ABORTED",
                        "reason": reason
                    })),
                }
            }

            "agent_2pc_stage_diff" => {
                let tid_str = args.get("tid").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: tid".into())
                })?;
                let path_str = args.get("path").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: path".into())
                })?;
                let old_string = args.get("old_string").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: old_string".into())
                })?;
                let new_string = args.get("new_string").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: new_string".into())
                })?;

                let tid = TransactionId::parse(tid_str)?;
                let dest_path = PathBuf::from(path_str);
                let pid = format!("fs_replace_{}", dest_path.file_name().unwrap_or_default().to_string_lossy());

                let mut adapter = FsAdapter::new(pid);
                let intent = ResourceIntent::FsReplace {
                    path: dest_path,
                    old_string: old_string.to_string(),
                    new_string: new_string.to_string(),
                };

                let vote = coord.prepare_participant(&tid, &mut adapter, intent)?;
                match vote {
                    ParticipantVote::Prepared(receipt) => Ok(json!({
                        "status": "ok",
                        "vote": "PREPARED",
                        "receipt": receipt
                    })),
                    ParticipantVote::Aborted { reason } => Ok(json!({
                        "status": "error",
                        "vote": "ABORTED",
                        "reason": reason
                    })),
                }
            }

            "agent_2pc_stage_cmd" => {
                let tid_str = args.get("tid").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: tid".into())
                })?;
                let command = args.get("command").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: command".into())
                })?;

                let tid = TransactionId::parse(tid_str)?;
                let mut adapter = CommandAdapter::new("cmd_exec");
                let intent = ResourceIntent::Command {
                    command: command.to_string(),
                    working_dir: std::env::current_dir().unwrap_or_default(),
                    env: HashMap::new(),
                };

                let vote = coord.prepare_participant(&tid, &mut adapter, intent)?;
                match vote {
                    ParticipantVote::Prepared(receipt) => Ok(json!({
                        "status": "ok",
                        "vote": "PREPARED",
                        "receipt": receipt
                    })),
                    ParticipantVote::Aborted { reason } => Ok(json!({
                        "status": "error",
                        "vote": "ABORTED",
                        "reason": reason
                    })),
                }
            }

            "agent_2pc_stage_api" => {
                let tid_str = args.get("tid").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: tid".into())
                })?;
                let endpoint = args.get("endpoint").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: endpoint".into())
                })?;
                let method = args.get("method").and_then(|v| v.as_str()).unwrap_or("POST").to_string();
                let key = args.get("idempotency_key").and_then(|v| v.as_str()).map(|s| s.to_string()).unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                let comp_ep = args.get("compensation_endpoint").and_then(|v| v.as_str()).map(|s| s.to_string());

                let tid = TransactionId::parse(tid_str)?;
                let mut adapter = ApiAdapter::new("api_lease");
                let intent = ResourceIntent::RestApi {
                    endpoint: endpoint.to_string(),
                    method,
                    headers: HashMap::new(),
                    body: None,
                    idempotency_key: key.clone(),
                    compensation_endpoint: comp_ep,
                    compensation_method: Some("DELETE".to_string()),
                    compensation_body: None,
                };

                let vote = coord.prepare_participant(&tid, &mut adapter, intent)?;
                match vote {
                    ParticipantVote::Prepared(receipt) => Ok(json!({
                        "status": "ok",
                        "vote": "PREPARED",
                        "idempotency_key": key,
                        "receipt": receipt
                    })),
                    ParticipantVote::Aborted { reason } => Ok(json!({
                        "status": "error",
                        "vote": "ABORTED",
                        "reason": reason
                    })),
                }
            }

            "agent_2pc_commit" => {
                let tid_str = args.get("tid").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: tid".into())
                })?;
                let tid = TransactionId::parse(tid_str)?;

                let tx = coord.get_status(&tid).ok_or(TxError::TransactionNotFound(tid))?.clone();
                let mut adapters = crate::adapters::reconstruct_adapters(&tx.participants);

                coord.commit(&tid, &mut adapters[..])?;
                Ok(json!({
                    "status": "ok",
                    "decision": "COMMITTED",
                    "tid": tid.to_string()
                }))
            }

            "agent_2pc_abort" => {
                let tid_str = args.get("tid").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: tid".into())
                })?;
                let reason = args.get("reason").and_then(|v| v.as_str()).unwrap_or("Manual MCP abort");
                let tid = TransactionId::parse(tid_str)?;

                let tx = coord.get_status(&tid).ok_or(TxError::TransactionNotFound(tid))?.clone();
                let mut adapters = crate::adapters::reconstruct_adapters(&tx.participants);

                coord.abort(&tid, reason, &mut adapters[..])?;
                Ok(json!({
                    "status": "ok",
                    "decision": "ABORTED",
                    "tid": tid.to_string(),
                    "reason": reason
                }))
            }

            "agent_2pc_status" => {
                if let Some(tid_str) = args.get("tid").and_then(|v| v.as_str()) {
                    let tid = TransactionId::parse(tid_str)?;
                    if let Some(tx) = coord.get_status(&tid) {
                        Ok(json!({
                            "tid": tx.tid.to_string(),
                            "state": tx.state.to_string(),
                            "participants": tx.participants.iter().map(|p| &p.0).collect::<Vec<_>>(),
                            "receipts_count": tx.receipts.len(),
                            "created_at": tx.created_at
                        }))
                    } else {
                        Err(TxError::TransactionNotFound(tid))
                    }
                } else {
                    let active = coord.active_transactions();
                    let list: Vec<_> = active
                        .iter()
                        .map(|t| {
                            json!({
                                "tid": t.tid.to_string(),
                                "state": t.state.to_string(),
                                "participants_count": t.participants.len()
                            })
                        })
                        .collect();
                    Ok(json!(list))
                }
            }

            "agent_2pc_record_read" => {
                let tid_str = args.get("tid").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: tid".into())
                })?;
                let path_str = args.get("path").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: path".into())
                })?;

                let tid = TransactionId::parse(tid_str)?;
                let hash = coord.record_read(&tid, path_str)?;
                Ok(json!({
                    "status": "ok",
                    "tid": tid.to_string(),
                    "path": path_str,
                    "hash": format!("0x{:08x}", hash)
                }))
            }

            "agent_2pc_compact" => {
                let report = coord.compact()?;
                Ok(serde_json::to_value(report)?)
            }

            "agent_2pc_savepoint" => {
                let tid_str = args.get("tid").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: tid".into())
                })?;
                let name_str = args.get("name").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: name".into())
                })?;

                let tid = TransactionId::parse(tid_str)?;
                let sp = coord.savepoint(&tid, name_str)?;
                Ok(serde_json::to_value(sp)?)
            }

            "agent_2pc_rollback_to" => {
                let tid_str = args.get("tid").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: tid".into())
                })?;
                let name_str = args.get("name").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: name".into())
                })?;

                let tid = TransactionId::parse(tid_str)?;
                let mut dummy_resources: Vec<Box<dyn ResourceManager>> = Vec::new();
                let unwound = coord.rollback_to(&tid, name_str, &mut dummy_resources[..])?;
                Ok(json!({
                    "status": "ok",
                    "tid": tid_str,
                    "savepoint": name_str,
                    "unwound_participants": unwound
                }))
            }

            "agent_2pc_exec" => {
                let tid_str = args.get("tid").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: tid".into())
                })?;
                let cmd_str = args.get("command").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: command".into())
                })?;
                let dir_str = args.get("dir").and_then(|v| v.as_str());

                let tid = TransactionId::parse(tid_str)?;
                let working_dir = dir_str.map(PathBuf::from).unwrap_or_else(|| PathBuf::from("."));
                let cmd_parts = vec![cmd_str.to_string()];

                let report = crate::exec::ExecRunner::execute_and_stage(
                    &mut coord,
                    &tid,
                    working_dir,
                    &cmd_parts,
                )?;
                Ok(serde_json::to_value(report)?)
            }

            "agent_2pc_overlay_exec" => {
                let tid_str = args.get("tid").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: tid".into())
                })?;
                let cmd_str = args.get("command").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: command".into())
                })?;
                let dir_str = args.get("dir").and_then(|v| v.as_str());

                let tid = TransactionId::parse(tid_str)?;
                let working_dir = dir_str.map(PathBuf::from).unwrap_or_else(|| PathBuf::from("."));
                let cmd_parts = vec![cmd_str.to_string()];

                let report = crate::overlay::OverlayRunner::execute_and_stage(
                    &mut coord,
                    &tid,
                    working_dir,
                    &cmd_parts,
                )?;
                Ok(serde_json::to_value(report)?)
            }

            "agent_2pc_daemon_status" => {
                let sock_path = self.tx_dir.join("agent-2pc.sock");
                let alive = crate::ipc::IpcClient::is_alive(&sock_path);
                Ok(json!({
                    "alive": alive,
                    "socket": sock_path.display().to_string()
                }))
            }

            "agent_2pc_diff" => {
                let tid_str = args.get("tid").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: tid".into())
                })?;
                let stat = args.get("stat").and_then(|v| v.as_bool()).unwrap_or(false);
                let tid = TransactionId::parse(tid_str)?;
                let diff = crate::diff::TransactionDiffer::diff_transaction(&coord, &tid)?;
                if stat {
                    Ok(json!({
                        "status": "ok",
                        "tid": tid_str,
                        "stat": diff.render_stat(),
                        "total_additions": diff.total_additions,
                        "total_deletions": diff.total_deletions,
                        "files_changed": diff.files.len()
                    }))
                } else {
                    Ok(json!({
                        "status": "ok",
                        "tid": tid_str,
                        "diff": diff.render_terminal(false),
                        "total_additions": diff.total_additions,
                        "total_deletions": diff.total_deletions,
                        "files_changed": diff.files.len()
                    }))
                }
            }

            "agent_2pc_export" => {
                let tid_str = args.get("tid").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: tid".into())
                })?;
                let tid = TransactionId::parse(tid_str)?;
                let out_path = args.get("output").and_then(|v| v.as_str())
                    .map(PathBuf::from)
                    .unwrap_or_else(|| PathBuf::from(format!("{}.a2pc", tid)));
                let report = crate::bundle::BundleManager::export_bundle(&coord, &tid, &out_path)?;
                Ok(json!({
                    "status": "ok",
                    "report": report
                }))
            }

            "agent_2pc_import" => {
                let file_str = args.get("file").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: file".into())
                })?;
                let report = crate::bundle::BundleManager::import_bundle(&mut coord, file_str)?;
                Ok(json!({
                    "status": "ok",
                    "report": report
                }))
            }

            "agent_2pc_hook_check" => {
                let report = crate::git::GitIntegrator::check_invariants(&self.tx_dir)?;
                Ok(serde_json::to_value(report)?)
            }

            "agent_2pc_git_commit" => {
                let tid_str = args.get("tid").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: tid".into())
                })?;
                let message = args.get("message").and_then(|v| v.as_str()).ok_or_else(|| {
                    TxError::Other("Missing required argument: message".into())
                })?;
                let tid = TransactionId::parse(tid_str)?;
                let report = crate::git::GitIntegrator::git_commit(&mut coord, &tid, message, None)?;
                Ok(serde_json::to_value(report)?)
            }

            unknown => Err(TxError::Other(format!("Unknown tool: {}", unknown))),
        }
    }
}
