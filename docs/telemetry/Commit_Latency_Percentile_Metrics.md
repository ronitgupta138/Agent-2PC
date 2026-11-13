# Distributed 2PC Transaction Latency & Telemetry Metrics Specification

## 1. Exported Performance Indicators
Agent-2PC instruments transaction phases for OpenTelemetry / Prometheus export:
- `agent2pc_prepare_duration_ms`: Duration from proposal emission to final vote receipt.
- `agent2pc_commit_duration_ms`: Duration of Phase 2 persistent commit execution.
- `agent2pc_abort_rate`: Ratio of aborted transactions to total initiated transactions.

## 2. SLA Performance Target
P99 two-phase commit latency across a 5-node local cluster must complete within $\le 25\,\text{ms}$.
