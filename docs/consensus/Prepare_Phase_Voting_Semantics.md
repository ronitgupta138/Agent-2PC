# Prepare Phase Voting Semantics

## Overview
Technical specification for **Agent-2PC** systems engine.

## Specification Details
Unanimous agreement requirement: if any participant votes VOTE_ABORT, global abort fires.

## Invariants & Design Guarantees
- Zero-cost abstractions where possible.
- Safe Rust memory semantics without undefined behavior (`unsafe` strictly audited).
- Deterministic behavior under concurrent thread interleavings.
