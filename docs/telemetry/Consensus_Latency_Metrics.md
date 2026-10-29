# Consensus Latency Prometheus Metrics

## Overview
Technical specification for **Agent-2PC** systems engine.

## Specification Details
Histograms tracking prepare duration, commit duration, and round-trip RPC latency.

## Invariants & Design Guarantees
- Zero-cost abstractions where possible.
- Safe Rust memory semantics without undefined behavior (`unsafe` strictly audited).
- Deterministic behavior under concurrent thread interleavings.
