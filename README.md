# `agent-2pc` ⚡

> **Heterogeneous Two-Phase Commit (2PC) & Compensating Journaling Engine for AI Agent Tool Execution.**

[![Crates.io](https://img.shields.io/badge/crates.io-v0.1.0-0891b2.svg)](https://crates.io/crates/agent-2pc)
[![License: MIT](https://img.shields.io/badge/License-MIT-0891b2.svg)](LICENSE)
[![Rust: 1.99+](https://img.shields.io/badge/Rust-1.99+-0891b2.svg)](https://www.rust-lang.org)
[![Build Status](https://img.shields.io/badge/Tests-5%2F5%20Passing-0891b2.svg)](#verification--test-suite)
[![Architecture: Crash--Consistent%20WAL](https://img.shields.io/badge/WAL-CRC32%20Fsync-0891b2.svg)](#write-ahead-log-specification)

---

## 1. Problem Statement

Autonomous AI coding agents (Hermes, Claude Code, OpenCode, SWE-Agent) operate as distributed orchestrators across non-transactional boundaries:
- **Filesystem mutations:** `patch`, `write_file`, directory creation.
- **Subprocess commands:** `cargo test`, `npm run build`, database migrations.
- **Remote APIs:** Issue trackers (Linear, Jira, GitHub), Slack notifications, payment leases.

When an agent executes a 5-step task and encounters an unexpected failure at step 4 (e.g., compile error, lint violation, network timeout, or invalid API key), the workspace is left in a **dirty, partially committed state**:
1. Files are half-edited with broken syntax.
2. Migrations or build artifacts pollute the workspace.
3. The Linear or GitHub issue was already marked "In Progress".
4. Prompt-level "Sagas" hallucinate, execute rollbacks out of order, or miss modified files.

`agent-2pc` eliminates dirty agent state by enforcing a formal, crash-consistent **Two-Phase Commit (2PC)** protocol over heterogeneous agent tool pipelines.

---

## 2. Technical Architecture & State Machine

```
              [ User Goal / Autonomous Agent Turn ]
                                |
                                v
+---------------------------------------------------------------+
|             `agent-2pc` Transaction Coordinator               |
|  - Global Transaction ID (TID) & Monotonic Epoch Tracking     |
|  - Append-Only Binary Write-Ahead Log (`.agent-tx/wal.bin`)   |
|  - CRC32 Checksum Validation on Every Record                  |
+---------------------------------------------------------------+
                                |
        +-----------------------+-----------------------+
        | Phase 1: PREPARE(TID)                         | Phase 1: PREPARE(TID)
        v                                               v
+-------------------------------+               +-------------------------------+
|      FsAdapter (Staged)       |               |    CommandAdapter (Sandbox)   |
|  - Writes to `.agent-tx/`     |               |  - Runs in staged scratch dir |
|  - Generates `.bak` snapshots |               |  - Asserts exit code == 0     |
|  - Verifies file integrity    |               |  - Pipes logs to scratch file |
+-------------------------------+               +-------------------------------+
        |                                               |
        +-----------------------+-----------------------+
                                |
               [ All Resources Voted PREPARED? ]
                                |
                +---------------+---------------+
                | YES                           | NO (Timeout / Error)
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

## 3. Write-Ahead Log Specification

Every state transition is framed into an append-only binary log with strict `fsync` flushes:

```
+----------------+---------------+--------------------+------------------+-----------------------+
|  Magic (4B)    |  Version (1B) |  Payload Len (4B)  |    CRC32 (4B)    |    JSON Payload (NB)  |
|  `0x41325043`  |      `1`      |  Little-Endian u32 |  Little-Endian   |  Serialized Record    |
+----------------+---------------+--------------------+------------------+-----------------------+
```

### Crash Recovery Guarantees
- **`PREPARED` with `CommitDecision` logged:** The coordinator crashed mid-commit. On reboot, `agent-2pc recover` executes **roll-forward** to finalize all participants.
- **`PREPARED` or `ACTIVE` without `CommitDecision`:** The coordinator crashed during execution. On reboot, `agent-2pc recover` executes **rollback** to restore original files and release leases.
- **Corrupted Record:** Detected via hardware-accelerated CRC32 mismatch; isolates torn records cleanly without propagating corruption.

---

## 4. Supported Adapters

| Adapter | Domain | Phase 1: `PREPARE` | Phase 2: `COMMIT` | Phase 2: `ABORT` |
| :--- | :--- | :--- | :--- | :--- |
| **`FsAdapter`** | Filesystem | Stages new files in scratch directory; captures `.bak` snapshot if target exists. | Atomically renames staged file to destination; cleans up scratch. | Restores original from `.bak` snapshot; purges scratch directory. |
| **`CommandAdapter`** | Subprocesses | Executes command in isolated scratch directory; asserts exit code 0. | Promotes generated build artifacts to target paths. | Kills hanging processes; discards scratch outputs. |
| **`ApiAdapter`** | External APIs | Allocates idempotency key; verifies payload schema & pre-auth. | Finalizes idempotency lease with remote server. | Calls registered compensation endpoint with rollback token. |
| **`GitAdapter`** | Version Control| Verifies repository clean state; creates detached temporary ref. | Fast-forwards / commits changes into target branch. | Discards temporary ref without altering working tree. |

---

## 5. Installation & Quickstart

### Build from Source
```bash
git clone https://github.com/ronitgupta138/agent-2pc.git
cd agent-2pc
cargo build --release
```

### CLI Workflow

#### 1. Initialize Workspace
```bash
agent-2pc init
```

#### 2. Begin Transaction
```bash
TID=$(agent-2pc begin --json | jq -r .tid)
echo "Transaction ID: $TID"
```

#### 3. Stage Operations in Phase 1
```bash
# Stage file write
agent-2pc stage-write --tid "$TID" --path "src/auth.rs" --content "pub fn authenticate() -> bool { true }"

# Stage command verification
agent-2pc stage-cmd --tid "$TID" --command "cargo check"

# Stage API lease
agent-2pc stage-api --tid "$TID" --endpoint "https://api.linear.app/graphql" --idempotency-key "key-123"
```

#### 4. Commit or Abort in Phase 2
```bash
# All steps passed: Atomic commit
agent-2pc commit --tid "$TID"

# Or if any step failed: Deterministic rollback
agent-2pc abort --tid "$TID" --reason "Unit test failed"
```

---

## 6. One-Shot Batch Execution for AI Agents

AI agent loops (Hermes, Claude Code, OpenCode) can pass an atomic multi-step transaction manifest:

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

If `cmd_test` fails, `agent-2pc` automatically discards `src/routes/user.rs` before it ever touches your repository.

---

## 7. Verification & Test Suite

The test suite validates 2PC invariants, WAL durability, CRC32 tamper detection, and crash recovery:

```bash
cargo test
```

```
running 5 tests
test test_wal_framing_and_crc32_tamper_detection ... ok
test test_api_adapter_lease_and_compensation ... ok
test test_crash_recovery_uncommitted_rollback ... ok
test test_2pc_partial_failure_deterministic_rollback ... ok
test test_2pc_all_prepared_atomic_commit ... ok

test result: ok. 5 passed; 0 failed; 0 ignored; finished in 0.02s
```

---

## 8. License

MIT License. Designed & engineered by [Ronit Gupta](https://github.com/ronitgupta138).
