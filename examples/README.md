# `agent-2pc` Integration Examples

This directory provides ready-to-run integrations, recipes, and workflow examples for autonomous agent harnesses using `agent-2pc` v2.0.0.

---

## 1. Model Context Protocol (MCP) Integration

`agent-2pc` provides both a **native embedded Rust MCP server** (`agent-2pc mcp`) and a Python JSON-RPC 2.0 stdio adapter (`mcp_agent_server.py`).

### Option A: Native Embedded MCP Server (Recommended)
Add directly to Claude Desktop (`claude_desktop_config.json`):
```json
{
  "mcpServers": {
    "agent-2pc": {
      "command": "agent-2pc",
      "args": ["mcp"]
    }
  }
}
```

### Option B: Python MCP Server
```json
{
  "mcpServers": {
    "agent-2pc": {
      "command": "python3",
      "args": ["/path/to/agent-2pc/examples/mcp_agent_server.py"]
    }
  }
}
```

### Supported MCP Tools:
- `agent_2pc_begin`: Starts a transaction session; returns unique `tid`.
- `agent_2pc_record_read`: Registers OCC read-set fingerprint on files before modifications.
- `agent_2pc_stage_write`: Stages file edits into isolated scratch memory without touching repository code.
- `agent_2pc_stage_patch`: Stages unified diff patches in scratch memory.
- `agent_2pc_stage_replace`: Stages targeted substring find-and-replace with uniqueness checks.
- `agent_2pc_stage_cmd`: Runs syntax checks, tests, or linters in scratch sandbox before commit.
- `agent_2pc_stage_api`: Reserves external API leases with compensation rollback tokens.
- `agent_2pc_diff`: Inspects unified diff and stats for staged file modifications.
- `agent_2pc_savepoint`: Marks a nested transactional checkpoint for speculative execution.
- `agent_2pc_rollback_to`: Unwinds staged steps back to a named savepoint without aborting the entire transaction.
- `agent_2pc_exec`: Executes arbitrary command and auto-stages any created or modified files into Phase 1.
- `agent_2pc_export`: Exports an active transaction to a verified portable `.a2pc` bundle.
- `agent_2pc_import`: Imports a verified `.a2pc` bundle as a prepared transaction.
- `agent_2pc_git_commit`: Atomically materializes 2PC transaction and creates a Git commit in one step.
- `agent_2pc_hook_check`: Verifies Git workspace integrity by asserting zero uncommitted transactions exist.
- `agent_2pc_status`: Inspects active transactions and receipts count.
- `agent_2pc_compact`: Purges finalized transactions from WAL and emits checkpoint marker.
- `agent_2pc_commit`: Atomically materializes all staged actions across all tools.
- `agent_2pc_abort`: Deterministically rolls back all staged work if any step fails.

---

## 2. Python SDK Workflow (`python_workflow.py`)

A full end-to-end Python script demonstrating initialization, OCC read-set registration, file staging, savepoints, rollback, bundle export, and atomic commit:

```bash
python3 examples/python_workflow.py
```

---

## 3. TypeScript SDK Workflow (`typescript_workflow.ts`)

A full end-to-end TypeScript script demonstrating workspace initialization, git integrity checks, staging, checkpointing, and atomic commitment:

```bash
node --experimental-strip-types examples/typescript_workflow.ts
```

---

## 4. One-Shot Batch Manifest Execution

When an agent plans multiple interdependent actions, pass a batch manifest directly:

```bash
agent-2pc run-batch --file examples/sample_batch.json
```

If any step fails (such as syntax verification or API validation), all other operations are rolled back instantly with zero leaked files.
