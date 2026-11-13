# Local Participant Scratchpad Isolation & Rollback Specification

## 1. Isolation Invariant
Agent actions during Phase 1 (`PREPARE`) must never mutate shared production state directly. All side-effects execute strictly within isolated transaction scratchpads.

## 2. Scratchpad Lifecycle
1. **Creation:** `create_scratchpad(tx_id)` provisions volatile workspace branch or memory segment.
2. **Execution:** File edits, API mutations, and database records staged inside scratchpad.
3. **Commit:** `commit(tx_id)` atomically merges scratchpad changes into main state.
4. **Abort:** `abort(tx_id)` recursively wipes scratchpad directory with zero side effects.
