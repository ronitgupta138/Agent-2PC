<div align="center">

```
 ▄▄▄        ▄████  ▓█████  ███▄    █ ▄▄▄█████▓    ██████  ██▓███   ▄████▄  
▒████▄     ██▒ ▀█▒ ▓█   ▀  ██ ▀█   █ ▓  ██▒ ▓▒  ▒██    ▒ ▓██░  ██▒▒██▀ ▀█  
▒██  ▀█▄  ▒██░▄▄▄░ ▒███   ▓██  ▀█ ██▒▒ ▓██░ ▒░  ░ ▓██▄   ▓██░ ██▓▒▒▓█    ▄ 
░██▄▄▄▄██ ░▓█  ██▓ ▒▓█  ▄ ▓██▒  ▐▌██▒░ ▓██▓ ░     ▒   ██▒▒██▄█▓▒ ▒▒▓▓▄ ▄██▒
 ▓█   ▓██▒░▒▓███▀▒ ░▒████▒▒██░   ▓██░  ▒██▒ ░   ▒██████▒▒▒██▒ ░  ░▒ ▓███▀ ░
 ▒▒   ▓▒█░ ░▒   ▒  ░░ ▒░ ░░ ▒░   ▒ ▒   ▒ ░░     ▒ ▒▓▒ ▒ ░▒▓▒░ ░  ░░ ░▒ ▒  ░
```

### ⚡ Two-Phase Commit & Compensating Journaling Engine for AI Agent Tool Execution

[![GitHub Stars](https://img.shields.io/github/stars/ronitgupta138/agent-2pc?style=flat-square&color=0891b2&labelColor=0a0b0e)](https://github.com/ronitgupta138/agent-2pc/stargazers)
[![Crates.io](https://img.shields.io/badge/crates.io-v0.1.0-0891b2?style=flat-square&labelColor=0a0b0e)](https://crates.io/crates/agent-2pc)
[![License: MIT](https://img.shields.io/badge/License-MIT-0891b2?style=flat-square&labelColor=0a0b0e)](LICENSE)
[![Rust: 1.80+](https://img.shields.io/badge/Rust-1.80+-0891b2?style=flat-square&labelColor=0a0b0e)](https://www.rust-lang.org)
[![Build Status](https://img.shields.io/badge/Tests-7%2F7%20Passing-0891b2?style=flat-square&labelColor=0a0b0e)](#-verification--test-suite)
[![Views](https://hits.sh/github.com/ronitgupta138/agent-2pc.svg?style=flat-square&label=Views&color=0891b2&labelColor=0a0b0e)](https://hits.sh/github.com/ronitgupta138/agent-2pc/)
[![Sponsor](https://img.shields.io/badge/Sponsor-%E2%9D%A4-ea4aaa?style=flat-square&labelColor=0a0b0e)](https://github.com/sponsors/ronitgupta138)

**Zero dirty state. Zero broken syntax. Zero hallucinated rollbacks.**  
A high-performance Rust coordinator that brings database ACID transactional finality to autonomous AI agents.

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

---

## 🤖 AI Agent & MCP (Model Context Protocol) Integration

`agent-2pc` includes a drop-in Model Context Protocol server (`examples/mcp_agent_server.py`) for **Claude Desktop, Cursor, and Hermes Agent**.

### Add to Claude Desktop (`claude_desktop_config.json`)
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

### One-Shot Batch Manifest Execution
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
running 7 tests
test test_wal_framing_and_crc32_tamper_detection ... ok
test test_api_adapter_lease_and_compensation ... ok
test test_crash_recovery_uncommitted_rollback ... ok
test test_2pc_partial_failure_deterministic_rollback ... ok
test test_2pc_all_prepared_atomic_commit ... ok
test test_wal_append_latency_benchmark ... ok
test test_end_to_end_2pc_transaction_throughput ... ok

test result: ok. 7 passed; 0 failed; 0 ignored; finished in 1.76s
```

---

## 📜 License

MIT License. Designed and engineered by [Ronit Gupta](https://github.com/ronitgupta138).
