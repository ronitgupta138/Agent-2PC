# Coordinator Node Launcher CLI

## Overview
Technical specification for **Agent-2PC** systems engine.

## Specification Details
CLI options for starting standalone transaction coordinator with raft backing.

## Invariants & Design Guarantees
- Zero-cost abstractions where possible.
- Safe Rust memory semantics without undefined behavior (`unsafe` strictly audited).
- Deterministic behavior under concurrent thread interleavings.
