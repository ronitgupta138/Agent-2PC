#!/usr/bin/env python3
"""
Model Context Protocol (MCP) Server for agent-2pc.

Exposes Two-Phase Commit (2PC) primitives to LLMs (Claude Desktop, Cursor, Hermes, AutoGen)
via standard JSON-RPC 2.0 over stdio.
"""

import sys
import json
import subprocess
import shutil

BIN_PATH = shutil.which("agent-2pc") or "./target/release/agent-2pc" or "./target/debug/agent-2pc"

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
                "content": {"type": "string", "description": "File text content"}
            },
            "required": ["tid", "path", "content"]
        }
    },
    {
        "name": "agent_2pc_stage_cmd",
        "description": "Phase 1: Run and verify a command in an isolated scratch sandbox before commit.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tid": {"type": "string", "description": "Active Transaction ID"},
                "command": {"type": "string", "description": "Shell command to verify"}
            },
            "required": ["tid", "command"]
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
            res = run_agent_2pc(["stage-write", "--tid", args["tid"], "--path", args["path"], "--content", args["content"]])
        elif name == "agent_2pc_stage_cmd":
            res = run_agent_2pc(["stage-cmd", "--tid", args["tid"], "--command", args["command"]])
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
