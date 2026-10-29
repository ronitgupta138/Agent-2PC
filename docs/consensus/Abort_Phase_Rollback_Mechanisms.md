# Abort Phase Compensating Rollbacks

## Overview
Technical specification for **Agent-2PC** systems engine.

## Specification Details
Compensating transaction triggers restoring participant state if global abort is declared.

## Invariants & Design Guarantees
- Zero-cost abstractions where possible.
- Safe Rust memory semantics without undefined behavior (`unsafe` strictly audited).
- Deterministic behavior under concurrent thread interleavings.
