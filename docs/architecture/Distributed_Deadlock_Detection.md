# Distributed Deadlock Detection & Cycle Breaking

## 1. Wait-For Graph (WFG) Analysis
- Background supervisor periodically constructs a directed Wait-For Graph representing active resource lock contentions.
- Cycle detection using Tarjan's strongly connected components algorithm triggers targeted rollback of the youngest transaction.
