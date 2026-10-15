#!/usr/bin/env python3
"""
Model Context Protocol (MCP) Server for agent-2pc.

Exposes Two-Phase Commit (2PC) primitives to LLMs (Claude Desktop, Cursor, Hermes, AutoGen)
via standard JSON-RPC 2.0 over stdio. Compatible with agent-2pc v2.0.0.
"""

import sys
import os
import json
import subprocess
import shutil

BIN_PATH = (
    shutil.which("agent-2pc")
    or os.path.abspath(os.path.join(os.path.dirname(__file__), "../target/release/agent-2pc"))
    or os.path.abspath(os.path.join(os.path.dirname(__file__), "../target/debug/agent-2pc"))
)

def run_agent_2pc(args):
    cmd = [BIN_PATH, "--json"] + args
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return {"error": result.stderr.strip() or result.stdout.strip()}
    try:
        return json.loads(result.stdout)
    except Exception:
        return {"output": result.stdout.strip()}

TOOLS = [
    {
        "name": "agent_2pc_begin",
        "description": "Start a new 2PC transaction and obtain a unique Transaction ID (TID).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "timeout_ms": {"type": "integer", "description": "Transaction timeout in milliseconds (default: 30000)"}
            }
        }
    },
    {
        "name": "agent_2pc_stage_write",
        "description": "Phase 1: Stage a file write into isolated scratch space with automatic .bak snapshotting.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tid": {"type": "string", "description": "Active Transaction ID"},
                "path": {"type": "string", "description": "Destination file path"},
                "content": {"type": "string", "description": "File text content"},
                "create_dirs": {"type": "boolean", "description": "Create parent directories if missing (default: true)"}
            },
            "required": ["tid", "path", "content"]
        }
    },
    {
        "name": "agent_2pc_stage_patch",
        "description": "Phase 1: Stage a unified diff patch into isolated scratch memory.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tid": {"type": "string", "description": "Active Transaction ID"},
                "path": {"type": "string", "description": "Destination file path"},
                "patch": {"type": "string", "description": "Unified diff patch content"}
            },
            "required": ["tid", "path", "patch"]
        }
    },
    {
        "name": "agent_2pc_stage_replace",
        "description": "Phase 1: Stage targeted unique substring replacement with uniqueness validation.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tid": {"type": "string", "description": "Active Transaction ID"},
                "path": {"type": "string", "description": "Destination file path"},
                "old_string": {"type": "string", "description": "Exact unique string to replace"},
                "new_string": {"type": "string", "description": "Replacement string"}
            },
            "required": ["tid", "path", "old_string", "new_string"]
        }
    },
    {
        "name": "agent_2pc_stage_cmd",
        "description": "Phase 1: Run and verify a command in an isolated scratch sandbox before commit.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tid": {"type": "string", "description": "Active Transaction ID"},
                "command": {"type": "string", "description": "Shell command to verify"},
                "dir": {"type": "string", "description": "Working directory (default: current dir)"}
            },
            "required": ["tid", "command"]
        }
    },
    {
        "name": "agent_2pc_stage_api",
        "description": "Phase 1: Stage external REST API mutation with idempotency lease & compensation hook.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tid": {"type": "string", "description": "Active Transaction ID"},
                "endpoint": {"type": "string", "description": "Target endpoint URL"},
                "method": {"type": "string", "description": "HTTP method (POST, PUT, DELETE, GET)"},
                "idempotency_key": {"type": "string", "description": "Custom idempotency key"},
                "compensation_endpoint": {"type": "string", "description": "Compensation rollback URL"}
            },
            "required": ["tid", "endpoint"]
        }
    },
    {
        "name": "agent_2pc_record_read",
        "description": "Record file hash in transaction read-set for optimistic concurrency conflict detection (OCC).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tid": {"type": "string", "description": "Active Transaction ID"},
                "path": {"type": "string", "description": "Path to read file"}
            },
            "required": ["tid", "path"]
        }
    },
    {
        "name": "agent_2pc_diff",
        "description": "Inspect unified diff and summary stats for all staged file modifications.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tid": {"type": "string", "description": "Active Transaction ID"},
                "stat": {"type": "boolean", "description": "Return summary diffstat only"}
            },
            "required": ["tid"]
        }
    },
    {
        "name": "agent_2pc_savepoint",
        "description": "Create a transactional savepoint checkpoint within an active transaction.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tid": {"type": "string", "description": "Active Transaction ID"},
                "name": {"type": "string", "description": "Savepoint name identifier"}
            },
            "required": ["tid", "name"]
        }
    },
    {
        "name": "agent_2pc_rollback_to",
        "description": "Roll back transaction changes staged after the specified savepoint.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tid": {"type": "string", "description": "Active Transaction ID"},
                "name": {"type": "string", "description": "Savepoint name to restore"}
            },
            "required": ["tid", "name"]
        }
    },
    {
        "name": "agent_2pc_exec",
        "description": "Execute arbitrary command and auto-stage any created or modified files into Phase 1.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tid": {"type": "string", "description": "Active Transaction ID"},
                "command": {"type": "string", "description": "Command to execute"},
                "dir": {"type": "string", "description": "Working directory (default: current dir)"}
            },
            "required": ["tid", "command"]
        }
    },
    {
        "name": "agent_2pc_export",
        "description": "Export an active transaction to a portable verified .a2pc bundle.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tid": {"type": "string", "description": "Active Transaction ID"},
                "output": {"type": "string", "description": "Output bundle file path (.a2pc)"}
            },
            "required": ["tid"]
        }
    },
    {
        "name": "agent_2pc_import",
        "description": "Import a transaction bundle (.a2pc) and register as a prepared transaction.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "file": {"type": "string", "description": "Bundle file path (.a2pc)"}
            },
            "required": ["file"]
        }
    },
    {
        "name": "agent_2pc_git_commit",
        "description": "Atomically commit 2PC transaction and create a Git commit in one step.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tid": {"type": "string", "description": "Active Transaction ID"},
                "message": {"type": "string", "description": "Git commit message"},
                "dir": {"type": "string", "description": "Git repository directory"}
            },
            "required": ["tid", "message"]
        }
    },
    {
        "name": "agent_2pc_hook_check",
        "description": "Check whether workspace is clean of pending uncommitted 2PC transactions.",
        "inputSchema": {
            "type": "object",
            "properties": {}
        }
    },
    {
        "name": "agent_2pc_status",
        "description": "Inspect active transaction states and participants.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tid": {"type": "string", "description": "Optional specific Transaction ID"}
            }
        }
    },
    {
        "name": "agent_2pc_compact",
        "description": "Compact WAL by purging finalized transactions while preserving active in-flight state.",
        "inputSchema": {
            "type": "object",
            "properties": {}
        }
    },
    {
        "name": "agent_2pc_commit",
        "description": "Phase 2: Globally and atomically commit all staged operations.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tid": {"type": "string", "description": "Active Transaction ID"}
            },
            "required": ["tid"]
        }
    },
    {
        "name": "agent_2pc_abort",
        "description": "Phase 2: Deterministically roll back all staged changes and restore backups.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tid": {"type": "string", "description": "Active Transaction ID"},
                "reason": {"type": "string", "description": "Reason for aborting"}
            },
            "required": ["tid"]
        }
    }
]

def handle_rpc(line):
    try:
        req = json.loads(line)
    except Exception:
        return

    req_id = req.get("id")
    method = req.get("method")
    params = req.get("params", {})

    if method == "tools/list":
        res = {"tools": TOOLS}
    elif method == "tools/call":
        name = params.get("name")
        args = params.get("arguments", {})
        if name == "agent_2pc_begin":
            timeout = args.get("timeout_ms", 30000)
            res = run_agent_2pc(["begin", "--timeout", str(timeout)])
        elif name == "agent_2pc_stage_write":
            cmd = ["stage-write", "--tid", args["tid"], "--path", args["path"], "--content", args["content"]]
            if args.get("create_dirs", True):
                cmd.append("--create-dirs")
            res = run_agent_2pc(cmd)
        elif name == "agent_2pc_stage_patch":
            res = run_agent_2pc(["stage-patch", "--tid", args["tid"], "--path", args["path"], f"--patch={args['patch']}"])
        elif name in ("agent_2pc_stage_replace", "agent_2pc_stage_diff"):
            res = run_agent_2pc(["stage-replace", "--tid", args["tid"], "--path", args["path"], "--old-string", args["old_string"], "--new-string", args["new_string"]])
        elif name == "agent_2pc_stage_cmd":
            cmd = ["stage-cmd", "--tid", args["tid"], "--command", args["command"]]
            if args.get("dir"):
                cmd.extend(["--dir", args["dir"]])
            res = run_agent_2pc(cmd)
        elif name == "agent_2pc_stage_api":
            cmd = ["stage-api", "--tid", args["tid"], "--endpoint", args["endpoint"], "--method", args.get("method", "POST")]
            if args.get("idempotency_key"):
                cmd.extend(["--idempotency-key", args["idempotency_key"]])
            if args.get("compensation_endpoint"):
                cmd.extend(["--compensation-endpoint", args["compensation_endpoint"]])
            res = run_agent_2pc(cmd)
        elif name == "agent_2pc_record_read":
            res = run_agent_2pc(["record-read", "--tid", args["tid"], "--path", args["path"]])
        elif name == "agent_2pc_diff":
            cmd = ["diff", "--tid", args["tid"]]
            if args.get("stat"):
                cmd.append("--stat")
            res = run_agent_2pc(cmd)
        elif name == "agent_2pc_savepoint":
            res = run_agent_2pc(["savepoint", "--tid", args["tid"], "--name", args["name"]])
        elif name == "agent_2pc_rollback_to":
            res = run_agent_2pc(["rollback-to", "--tid", args["tid"], "--name", args["name"]])
        elif name == "agent_2pc_exec":
            cmd = ["exec", "--tid", args["tid"]]
            if args.get("dir"):
                cmd.extend(["--dir", args["dir"]])
            cmd.extend(["--", "sh", "-c", args["command"]])
            res = run_agent_2pc(cmd)
        elif name == "agent_2pc_export":
            cmd = ["export", "--tid", args["tid"]]
            if args.get("output"):
                cmd.extend(["--output", args["output"]])
            res = run_agent_2pc(cmd)
        elif name == "agent_2pc_import":
            res = run_agent_2pc(["import", "--file", args["file"]])
        elif name == "agent_2pc_git_commit":
            cmd = ["git-commit", "--tid", args["tid"], f"--message={args['message']}"]
            if args.get("dir"):
                cmd.extend(["--dir", args["dir"]])
            res = run_agent_2pc(cmd)
        elif name == "agent_2pc_hook_check":
            res = run_agent_2pc(["hook", "check"])
        elif name == "agent_2pc_status":
            cmd = ["status"]
            if args.get("tid"):
                cmd.extend(["--tid", args["tid"]])
            res = run_agent_2pc(cmd)
        elif name == "agent_2pc_compact":
            res = run_agent_2pc(["compact"])
        elif name == "agent_2pc_commit":
            res = run_agent_2pc(["commit", "--tid", args["tid"]])
        elif name == "agent_2pc_abort":
            reason = args.get("reason", "Agent abort")
            res = run_agent_2pc(["abort", "--tid", args["tid"], "--reason", reason])
        else:
            res = {"error": f"Unknown tool: {name}"}
    else:
        res = {"error": f"Unsupported method: {method}"}

    response = {"jsonrpc": "2.0", "id": req_id, "result": res}
    sys.stdout.write(json.dumps(response) + "\n")
    sys.stdout.flush()

def main():
    for line in sys.stdin:
        if line.strip():
            handle_rpc(line.strip())

if __name__ == "__main__":
    main()
