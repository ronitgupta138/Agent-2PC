# Two-Phase Commit (2PC) Protocol Invariants for AI Tool Calling

## 1. Context & Motivation
When an autonomous agent invokes multiple mutating tools in a single turn (e.g., updating a database record, writing a local configuration file, and dispatching an API webhook), an unhandled failure on step 3 leaves steps 1 and 2 partially applied. Agent-2PC brings transactional ACID semantics to heterogeneous LLM tool executions.

## 2. Protocol State Machine
- **Phase 1: Prepare (Can-Commit?)**
  - Coordinator broadcasts `PREPARE(tx_id, payload)` to all enrolled tool cohorts.
  - Each cohort acquires local resource locks, validates preconditions, appends a `PREPARE` entry to its local WAL, and responds with `VOTE_COMMIT` or `VOTE_ABORT`.
- **Phase 2: Commit or Abort**
  - **Global Commit:** If all cohorts reply `VOTE_COMMIT`, the coordinator writes `GLOBAL_COMMIT` to its WAL and broadcasts `COMMIT(tx_id)`. Cohorts apply changes permanently and release locks.
  - **Global Abort:** If ANY cohort votes `VOTE_ABORT` or times out (default: 3,000ms), coordinator broadcasts `ABORT(tx_id)`. All cohorts roll back changes to the pre-transaction snapshot.

## 3. Strict Safety Invariants
- **Non-blocking Abort:** Cohorts that vote `VOTE_ABORT` can unilaterally abort without awaiting coordinator instruction.
- **Durable Prepare:** Once a cohort replies `VOTE_COMMIT`, it MUST be able to complete the transaction even if the coordinator reboots.
