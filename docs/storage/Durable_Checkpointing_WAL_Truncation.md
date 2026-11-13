# Durable Checkpointing & WAL Truncation Specification

## 1. Problem Statement
Continuous operation produces gigabytes of transaction logs. Unbounded WAL growth lengthens crash recovery replay times.

## 2. Checkpoint Creation Protocol
1. Coordinator issues `CHECKPOINT_BEGIN` snapshot barrier.
2. Active state engine flushes in-memory participant tables to immutable disk snapshot (`snapshot.db`).
3. WAL log entries older than checkpoint LSN (Log Sequence Number) are safely truncated.

## 3. Bound Recovery Window
Guarantees cold-start crash recovery replays at most 1,000 log frames, achieving sub-50ms startup times.
