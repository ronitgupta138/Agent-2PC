# Participant Agent Lifecycle Protocol

## Overview
Technical specification for **Agent-2PC** systems engine.

## Specification Details
State transitions from idle to enrolled, prepared, and finalized.

## Invariants & Design Guarantees
- Zero-cost abstractions where possible.
- Safe Rust memory semantics without undefined behavior (`unsafe` strictly audited).
- Deterministic behavior under concurrent thread interleavings.
