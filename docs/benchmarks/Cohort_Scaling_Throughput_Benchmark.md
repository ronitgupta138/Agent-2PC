# Cohort Scaling & Consensus Throughput Benchmark Specification

## 1. Benchmark Setup
Measures peak transactions per second (TPS) and latency scaling across varying cluster cohort sizes.

## 2. Scaling Performance Matrix
| Cohort Size | Quorum Size | Max Throughput (TPS) | P99 Latency |
|---|---|---|---|
| 3 Nodes | 2 | $14,200\,\text{TPS}$ | $3.8\,\text{ms}$ |
| 5 Nodes | 3 | $9,800\,\text{TPS}$ | $6.2\,\text{ms}$ |
| 7 Nodes | 4 | $6,400\,\text{TPS}$ | $11.5\,\text{ms}$ |

## 3. Architectural Conclusion
5-node cohort provides optimal balance between high fault-tolerance (tolerates 2 simultaneous node crashes) and sub-10ms transaction latency.
