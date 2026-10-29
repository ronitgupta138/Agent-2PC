# Multi Agent Resource Deadlock Detection

## Overview
Technical specification for **Agent-2PC** systems engine.

## Specification Details
Wait-for-graph cycle detection and victim selection based on transaction age.

## Invariants & Design Guarantees
- Zero-cost abstractions where possible.
- Safe Rust memory semantics without undefined behavior (`unsafe` strictly audited).
- Deterministic behavior under concurrent thread interleavings.
