# Network Partition & Split-Brain Consensus Recovery Specification

## 1. Quorum Intersection
To prevent split-brain scenarios where isolated sub-clusters attempt conflicting commits, Agent-2PC enforces strict majority quorum:

$$Q = \left\lfloor \frac{N}{2} \right\rfloor + 1$$

## 2. Minority Partition Behavior
Participants in a minority partition detect lack of quorum responses and immediately transition to read-only passive mode, rejecting incoming mutation proposals.

## 3. Healing & State Reconciliation
Upon partition healing, minority nodes sync state against majority WAL logs using transaction sequence numbers.
