<div align="center">

```text
 █████╗  ██████╗ ███████╗███╗   ██╗████████╗   ██████╗ ██████╗  ██████╗
██╔══██╗██╔════╝ ██╔════╝████╗  ██║╚══██╔══╝   ╚════██╗██╔══██╗██╔════╝
███████║██║  ███╗█████╗  ██╔██╗ ██║   ██║       █████╔╝██████╔╝██║     
██╔══██║██║   ██║██╔══╝  ██║╚██╗██║   ██║      ██╔═══╝ ██╔═══╝ ██║     
██║  ██║╚██████╔╝███████╗██║ ╚████║   ██║      ███████╗██║     ╚██████╗
╚═╝  ╚═╝ ╚═════╝ ╚══════╝╚═╝  ╚═══╝   ╚═╝      ╚══════╝╚═╝      ╚═════╝
```

### **Two-Phase Commit & Compensating Journaling Engine for AI Agent Tool Execution**

[![GitHub Stars](https://img.shields.io/github/stars/ronitgupta138/agent-2pc?style=flat-square&color=0891b2&labelColor=0a0b0e)](https://github.com/ronitgupta138/agent-2pc/stargazers)
[![Crates.io](https://img.shields.io/badge/crates.io-v1.0.0-0891b2?style=flat-square&labelColor=0a0b0e)](https://crates.io/crates/agent-2pc)
[![License: MIT](https://img.shields.io/badge/License-MIT-0891b2?style=flat-square&labelColor=0a0b0e)](LICENSE)
[![Rust: 1.80+](https://img.shields.io/badge/Rust-1.80+-0891b2?style=flat-square&labelColor=0a0b0e)](https://www.rust-lang.org)
[![Build Status](https://img.shields.io/badge/Tests-24%2F24%20Passing-0891b2?style=flat-square&labelColor=0a0b0e)](#-verification--test-suite)
[![M8ven Score](https://m8ven.ai/badge/mcp/ronitgupta138-agent-2pc-d4r842?v=8f1b3351f3bf93b1547d4402c5552ea8)](https://m8ven.ai/mcp/ronitgupta138-agent-2pc-d4r842?s=readme)
[![Views](https://hits.sh/github.com/ronitgupta138/agent-2pc.svg?style=flat-square&label=Views&color=0891b2&labelColor=0a0b0e)](https://hits.sh/github.com/ronitgupta138/agent-2pc/)
[![Sponsor](https://img.shields.io/badge/Sponsor-%E2%9D%A4-ea4aaa?style=flat-square&labelColor=0a0b0e)](https://github.com/sponsors/ronitgupta138)

*Engineered for autonomous coding agents, tool orchestrators, and developer harnesses. Bringing database ACID transactional finality to agent tool execution with zero dirty state, sub-2ms transaction latency, and deterministic crash recovery.*

</div>

---

## 🎯 The Problem: Why Agent "Sagas" Fail

Autonomous coding agents (Hermes, Claude Code, OpenCode, SWE-Agent) operate across inherently non-transactional primitives:
- **Filesystem mutations:** `patch`, `write_file`, directory creation.
- **Subprocess commands:** `cargo test`, `npm run build`, database migrations.
- **Remote APIs:** Issue trackers (Linear, Jira, GitHub), Slack notifications, payment leases.

When an agent executes a 5-step task and encounters an unexpected failure at step 4 (a compiler error, test assertion, or API rate limit), existing frameworks rely on **prompt-level Sagas** where the model is asked to "clean up its work".

### The Result:
1. **Hallucinated Rollbacks:** The LLM forgets which files it touched, applies inverted diffs to the wrong line numbers, or deletes untouched user files.
2. **Dirty Repositories:** Syntax errors and half-baked imports remain committed in the user's working tree.
3. **Orphaned Cloud State:** Issues marked "In Progress" or API tokens remain dangling in third-party services.

`agent-2pc` solves this by introducing a formal, crash-consistent **Two-Phase Commit (2PC)** protocol with an append-only Write-Ahead Log (WAL) for AI tool pipelines.

---

## 📊 Architectural Comparison

| Feature | Naive Agent Execution | Prompt-Level Sagas | Git Stash / Branching | `agent-2pc` |
| :--- | :---: | :---: | :---: | :---: |
| **Rollback Reliability** | ❌ 0% (Manual user fix) | ⚠️ Unreliable (LLM hallucination) | ⚠️ Git files only | **100% Deterministic (WAL Replay)** |
| **Crash Consistency** | ❌ None | ❌ None | ❌ Uncommitted changes lost | **Fsync binary WAL + CRC32** |
| **Cross-Boundary Support** | ❌ None | ❌ Ad-hoc | ❌ Git only (no APIs/processes) | **VFS + Process + REST API** |
| **Overhead Latency** | 0ms | 1,500ms – 4,000ms (extra LLM calls) | 120ms – 350ms (git index locks) | **< 1.8ms (Zero LLM calls)** |
| **Model Context Pollution** | ❌ High (Tokens wasted on repair) | ❌ High (Retry loop debates) | ⚠️ Medium | **Zero (Clean kernel/VFS rollback)** |

---

## 🏛️ System Architecture

```
              [ User Goal / Autonomous Agent Turn ]
                                |
                                v
+---------------------------------------------------------------+
|             `agent-2pc` Transaction Coordinator               |
|  - Global Transaction ID (TID) & Monotonic Epoch Tracking     |
|  - Append-Only Binary Write-Ahead Log (`.agent-tx/wal.bin`)   |
|  - Hardware-Accelerated CRC32 Checksum on Every Record        |
+---------------------------------------------------------------+
                                |
        +-----------------------+-----------------------+
        | Phase 1: PREPARE(TID)                         | Phase 1: PREPARE(TID)
        v                                               v
+-------------------------------+               +-------------------------------+
|      FsAdapter (Staged)       |               |    CommandAdapter (Sandbox)   |
|  - Writes to `.agent-tx/`     |               |  - Runs in staged scratch dir |
|  - Generates `.bak` snapshots |               |  - Asserts exit code == 0     |
|  - Pre-verifies syntax        |               |  - Pipes logs to scratch file |
+-------------------------------+               +-------------------------------+
        |                                               |
        +-----------------------+-----------------------+
                                |
               [ All Resources Voted PREPARED? ]
                                |
                +---------------+---------------+
                | YES                           | NO (Timeout / Step Failed)
                v                               v
+-------------------------------+   +-------------------------------+
|     Phase 2: COMMIT(TID)      |   |      Phase 2: ABORT(TID)      |
|                               |   |                               |
| 1. Write `CommitDecision` WAL |   | 1. Write `AbortDecision` WAL  |
| 2. Atomic rename/promote files|   | 2. Revert files from `.bak`   |
| 3. Finalize API leases        |   | 3. Execute compensation hooks |
| 4. Clean up scratch directory |   | 4. Discard scratch directory  |
| 5. Mark `Committed` in WAL    |   | 5. Mark `Aborted` in WAL      |
+-------------------------------+   +-------------------------------+
```

---

## ⚡ Verified Benchmarks (AMD Ryzen 5 5600H / NVMe)

Measured via `cargo test --test benchmark -- --nocapture`:

```
⚡ 2PC Transaction Throughput: 114.4 tx/sec (200 full 2PC rounds with fsync)
⚡ WAL Fsync Latency:         p50=1.72ms | p95=1.81ms | p99=1.94ms
⚡ Rollback Neutralization:   < 210µs per aborted participant
```

---

## 📦 Supported Heterogeneous Adapters

| Adapter | Domain | Phase 1: `PREPARE` | Phase 2: `COMMIT` | Phase 2: `ABORT` |
| :--- | :--- | :--- | :--- | :--- |
| **`FsAdapter`** | Filesystem | Stages new files in scratch memory; captures `.bak` snapshot if target exists. | Atomically renames staged file to destination; purges scratch. | Restores original from `.bak` snapshot; purges scratch directory. |
| **`MicrovmAdapter`** | Hardware Virtualization | Executes untrusted code inside memory-caged MicroVM / KVM boundary with resource limits. | Promotes verified output artifacts cleanly. | Shuts down and destroys ephemeral VM disk/memory without host impact. |
| **`SandboxAdapter`** | Container Security | Runs commands inside unprivileged Linux user namespace & bubblewrap; mounts host read-only. | Promotes verified build artifacts from container workspace. | Kills container process hierarchy; purges ephemeral workspace. |
| **`GovernorAdapter`** | Velocity Defense | Enforces sliding-window task/runtime limits; fires in-kernel `SIGSTOP` on fork/delete spikes. | Promotes verified command outputs cleanly. | Freezes and terminates rogue process group (`SIGKILL`); rolls back staging. |
| **`CommandAdapter`** | Subprocesses | Executes command in isolated scratch directory; asserts exit code 0. | Promotes generated build artifacts to target paths. | Kills hanging processes; discards scratch outputs. |
| **`ApiAdapter`** | External APIs | Allocates idempotency key; verifies payload schema & pre-auth. | Finalizes idempotency lease with remote server. | Calls registered compensation endpoint with rollback token. |
| **`GitAdapter`** | Version Control| Verifies repository clean state; creates detached temporary ref. | Fast-forwards / commits changes into target branch. | Discards temporary ref without altering working tree. |

---

## 🚀 Quickstart

### 1. Installation
```bash
cargo install agent-2pc
# Or build from source
git clone https://github.com/ronitgupta138/agent-2pc.git
cd agent-2pc && cargo build --release
```

### 2. Standard CLI Usage
```bash
# 1. Initialize repository workspace
agent-2pc init

# 2. Begin transaction session
TID=$(agent-2pc begin --json | jq -r .tid)

# 3. Phase 1: Stage operations
agent-2pc stage-write --tid "$TID" --path "src/auth.rs" --content "pub fn auth() -> bool { true }"
agent-2pc stage-cmd --tid "$TID" --command "cargo check"

# 4. Phase 2: Atomic Commit (or Abort if any step failed)
agent-2pc commit --tid "$TID"
```

### 3. Full CLI Command Palette

| Subcommand | Description | Primary Use Case |
| :--- | :--- | :--- |
| `agent-2pc init` | Initialize `.agent-tx` workspace | Run once per repository root |
| `agent-2pc begin` | Open new transaction & return TID | Start of agent turn |
| `agent-2pc record-read` | Fingerprint file for OCC conflict detection | Before analyzing/reading code |
| `agent-2pc stage-write` | Stage file content with `.bak` snapshot | File creations & modifications |
| `agent-2pc stage-patch` | Stage unified diff in scratch memory | Targeted patch operations |
| `agent-2pc stage-cmd` | Verify command in staging directory | Linters, test suites, migrations |
| `agent-2pc stage-api` | Lease API idempotency token & compensation | Linear/GitHub/Slack mutations |
| `agent-2pc commit` | Atomically materialize all staged actions | All steps verified successfully |
| `agent-2pc abort` | Deterministically roll back staged changes | Step failure or invariant violation |
| `agent-2pc merge` | Three-way non-overlapping reconciliation | Concurrent agent edits |
| `agent-2pc top` / `monitor` | Live HUD (`--watch` for continuous auto-refresh)| Real-time developer observability |
| `agent-2pc recover` | Scan WAL and resolve uncommitted crashes | Post-crash startup recovery |
| `agent-2pc mcp` | Zero-dependency JSON-RPC 2.0 stdio server | Claude Desktop, Cursor, Hermes |
| `agent-2pc run-batch` | One-shot atomic execution from JSON manifest | Scripted multi-step workflows |

---

## 🤖 AI Agent & MCP (Model Context Protocol) Integration

`agent-2pc` includes a **zero-dependency embedded Model Context Protocol (MCP) server** running standard JSON-RPC 2.0 over stdio.

### Drop-in Setup for Claude Desktop (`claude_desktop_config.json`)
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

### Available MCP Tools:
- `agent_2pc_begin`: Starts a transaction session; returns `tid`.
- `agent_2pc_record_read`: Registers OCC read-set fingerprint on code files before generating modifications.
- `agent_2pc_stage_write`: Stages file edits into isolated scratch memory without touching repository code.
- `agent_2pc_stage_patch`: Stages unified diff patches in scratch memory.
- `agent_2pc_stage_cmd`: Runs syntax checks, tests, or linters in scratch sandbox before commit.
- `agent_2pc_stage_api`: Reserves external API leases with compensation tokens.
- `agent_2pc_commit`: Atomically materializes all staged actions across all tools.
- `agent_2pc_abort`: Deterministically rolls back all staged work if any step fails.
- `agent_2pc_status`: Inspects active transactions and receipts count.

---

## 🔒 Optimistic Concurrency Control (OCC) for Multi-Agent Swarms

When multiple autonomous agents or human developers edit the same repository concurrently, `agent-2pc` prevents **lost updates and silent overwrites**:

```bash
# 1. Agent reads a file and registers OCC fingerprint
agent-2pc record-read --tid "$TID" --path "src/auth.rs"

# 2. If another process modifies src/auth.rs while the agent was planning...
# 3. Phase 1 PREPARE automatically detects hash mismatch and aborts:
# ✖ ABORTED: Conflict detected: file src/auth.rs was modified concurrently
```

---

## 🖥️ Live Terminal Monitor & Transaction HUD (`top`)

Inspect in-flight transactions, WAL disk usage, and recovery health in real-time with continuous auto-refresh:

```bash
# Snapshot inspection
agent-2pc monitor

# Or continuous live dashboard (aliased as `top`)
agent-2pc top --watch --interval-ms 500
```

```text
┌────────────────────────────────────────────────────────────────────────┐
│  ⚡ agent-2pc Live Transaction Monitor & HUD                           │
├────────────────────────────────────────────────────────────────────────┤
│  Workspace      :: .agent-tx                                          │
│  WAL Log        :: 4.2 KB (18 records, CRC32 verified)                │
│  Scratch Dir    :: 0.0 KB in-flight                                   │
│  System State   :: HEALTHY (Durability verified, zero leaks)          │
├────────────────────────────────────────────────────────────────────────┤
│  Transactions: 1 Active  |  12 Committed  |  1 Aborted                 │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 🔀 Three-Way Semantic Merge Engine (`agent-2pc merge`)

When concurrent agents make non-overlapping edits to the same file, `agent-2pc` reconciles changes automatically rather than aborting:

```bash
agent-2pc merge --base ancestor.rs --ours committed.rs --theirs staged.rs --output merged.rs
```

---

## 🤖 One-Shot Batch Manifest Execution
AI agent planners can submit an entire multi-step transaction plan as an atomic JSON document:

```json
{
  "timeout_ms": 30000,
  "steps": [
    {
      "participant_id": "fs_route",
      "intent": {
        "type": "fs_write",
        "path": "src/routes/user.rs",
        "content": "// User Route Handlers",
        "create_dirs": true
      }
    },
    {
      "participant_id": "cmd_test",
      "intent": {
        "type": "command",
        "command": "cargo test --test user_test",
        "working_dir": ".",
        "env": {}
      }
    }
  ]
}
```

```bash
agent-2pc run-batch --file manifest.json
```

If `cmd_test` fails, `src/routes/user.rs` is discarded instantly before touching the repository.

---

## 💻 Client SDKs (Python & TypeScript)

### Python SDK (`agent-2pc`)
```python
from agent_2pc import Agent2PC

tx_client = Agent2PC()

# Automatic transactional context: commits on clean exit, rolls back on exception
with tx_client.transaction(timeout_ms=30000) as session:
    session.stage_write("src/auth.py", "def authenticate(): return True")
    session.stage_cmd("pytest tests/test_auth.py")
```

### TypeScript SDK (`agent-2pc`)
```typescript
import { Agent2PC } from "agent-2pc";

const client = new Agent2PC();
const session = await client.begin({ timeoutMs: 30000 });

await session.stageWrite("src/routes.ts", "// Route Handlers");
await session.stageCommand("npm run test");
await session.commit();
```

---

## 🛡️ Crash Consistency & Tamper Detection

Every record written to `.agent-tx/wal.bin` uses binary framing with a 32-bit hardware-accelerated CRC32 checksum:

```
+----------------+---------------+--------------------+------------------+-----------------------+
|  Magic (4B)    |  Version (1B) |  Payload Len (4B)  |    CRC32 (4B)    |    JSON Payload (NB)  |
|  `0x41325043`  |      `1`      |  Little-Endian u32 |  Little-Endian   |  Serialized Record    |
+----------------+---------------+--------------------+------------------+-----------------------+
```

If power is lost or the agent process is killed (`SIGKILL`):
```bash
agent-2pc recover
```
- Transactions with `CommitDecision` logged are **rolled forward** to finish execution.
- In-flight uncommitted transactions are **rolled back** and scratch files are cleaned up.
- Torn writes and bit-flips are trapped via CRC32 validation.

---

## 🧪 Verification & Test Suite

```bash
cargo test
```

```
running 24 tests
test test_reflink_or_copy_snapshot_staging ... ok
test test_monitor_stats_collection ... ok
test test_wal_framing_and_crc32_tamper_detection ... ok
test test_microvm_adapter_execution ... ok
test test_optimistic_concurrency_conflict_detection ... ok
test test_api_adapter_lease_and_compensation ... ok
test test_crash_recovery_uncommitted_rollback ... ok
test test_sandbox_adapter_filesystem_protection ... ok
test test_2pc_partial_failure_deterministic_rollback ... ok
test test_governor_adapter_clean_execution ... ok
test test_2pc_all_prepared_atomic_commit ... ok
test test_parallel_prepare_with_short_circuit_abort ... ok
test test_governor_adapter_timeout_containment ... ok
test test_mcp_initialize_and_tools_list ... ok
test test_mcp_begin_stage_commit_flow ... ok
test test_three_way_merge_non_overlapping_clean ... ok
test test_three_way_merge_overlapping_conflict ... ok
test test_crdt_commutativity ... ok
test test_crdt_deletion_tombstone ... ok
test test_crdt_concurrent_insertion_convergence ... ok
test test_distributed_consensus_all_prepared ... ok
test test_distributed_consensus_peer_abort ... ok
test test_wal_append_latency_benchmark ... ok
test test_end_to_end_2pc_transaction_throughput ... ok

test result: ok. 24 passed; 0 failed; 0 ignored; finished in 1.91s
```

---

## 📜 License

MIT License. Designed and engineered by [Ronit Gupta](https://github.com/ronitgupta138).
