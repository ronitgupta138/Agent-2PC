# Log Compaction and Checkpointing

## Overview
Technical specification for **Agent-2PC** systems engine.

## Specification Details
Fuzzy checkpointing truncating completed transactions and reclaiming log disk space.

## Invariants & Design Guarantees
- Zero-cost abstractions where possible.
- Safe Rust memory semantics without undefined behavior (`unsafe` strictly audited).
- Deterministic behavior under concurrent thread interleavings.
