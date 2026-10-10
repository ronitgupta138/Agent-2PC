# `agent-2pc` Integration Examples

This directory provides ready-to-run integrations and recipes for autonomous agent harnesses.

---

## 1. Model Context Protocol (MCP) Integration

The script `mcp_agent_server.py` exposes `agent-2pc` tools over standard stdio JSON-RPC 2.0.

### Adding to Claude Desktop (`claude_desktop_config.json`)
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

### Available MCP Tools:
- `agent_2pc_begin`: Starts a transaction session; returns `tid`.
- `agent_2pc_stage_write`: Stages file edits into isolated scratch memory without touching repository code.
- `agent_2pc_stage_cmd`: Runs syntax checks, tests, or linters in scratch sandbox before commit.
- `agent_2pc_commit`: Atomically materializes all staged actions across all tools.
- `agent_2pc_abort`: Deterministically rolls back all staged work if any step fails.

---

## 2. One-Shot Batch Manifest Execution

When an agent plans multiple interdependent actions, pass a batch manifest directly:

```bash
agent-2pc run-batch --file examples/sample_batch.json
```
