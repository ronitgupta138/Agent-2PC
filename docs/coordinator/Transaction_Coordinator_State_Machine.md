# Transaction Coordinator State Machine

## Overview
Technical specification for **Agent-2PC** systems engine.

## Specification Details
Explicit finite state machine states: Init -> Preparing -> Committed / Aborted -> Done.

## Invariants & Design Guarantees
- Zero-cost abstractions where possible.
- Safe Rust memory semantics without undefined behavior (`unsafe` strictly audited).
- Deterministic behavior under concurrent thread interleavings.
