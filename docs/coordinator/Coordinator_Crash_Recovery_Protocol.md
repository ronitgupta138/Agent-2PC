# Coordinator Crash Recovery Protocol

## Overview
Technical specification for **Agent-2PC** systems engine.

## Specification Details
Replaying WAL upon coordinator restart to resolve in-doubt transactions.

## Invariants & Design Guarantees
- Zero-cost abstractions where possible.
- Safe Rust memory semantics without undefined behavior (`unsafe` strictly audited).
- Deterministic behavior under concurrent thread interleavings.
