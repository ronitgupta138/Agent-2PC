# Scratchspace Diffing and Deterministic Rollback Algorithms

## 1. Filesystem Snapshotting
Before allowing an AI coding agent to modify local files, Agent-2PC computes a content SHA-256 manifest and creates hard links in an isolated `.agent-tx/<tx_id>/` tree.

## 2. Reverse Diff Generation
- For text files: Generates a unified reverse patch using Myers diff algorithm.
- For binary artifacts: Retains a copy-on-write sparse block snapshot.

## 3. Rollback Guarantees
- If a tool failure or user cancel occurs mid-operation, `agent-2pc rollback <tx_id>` restores the exact pre-transaction filesystem state in `< 50ms`.
