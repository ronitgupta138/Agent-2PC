# Coordinator Failure Detection & Distributed Lease Specification

## 1. Problem Statement
In multi-agent distributed systems governed by Two-Phase Commit (2PC), coordinator node failure during the `PREPARE` phase risks leaving participants indefinitely locked.

## 2. Heartbeat Lease Mechanism
- The coordinator renews an atomic lease every $T_{\text{heartbeat}} = 500\,\text{ms}$.
- Participants maintain failure detector timers with timeout window:
$$T_{\text{fail}} = 3 \cdot T_{\text{heartbeat}} + \text{Jitter}$$

## 3. Autonomous Abort on Lease Expiry
If the lease expires while a participant is in the `PREPARED` state, the participant safely initiates distributed inquiry or aborts to release local resource locks.
