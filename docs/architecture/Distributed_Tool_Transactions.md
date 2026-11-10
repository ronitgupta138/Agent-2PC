# Distributed MCP Tool Transaction Coordination Architecture

## 1. Network Topology
When an agent communicates with external MCP servers across local Unix domain sockets or HTTP/SSE streams:
- The coordinator injects an `X-Agent-Tx-Id` header on all outbound tool requests.
- Tool servers supporting the Agent-2PC protocol buffer changes in an uncommitted staging workspace until the final commit frame arrives.

## 2. Idempotency Key Guarantees
Every tool call carries a deterministic UUID v5 generated from `(tx_id, tool_name, arguments_hash)` to protect against network retry duplicate executions.
