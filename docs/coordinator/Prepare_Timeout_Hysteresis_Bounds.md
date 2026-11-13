# Two-Phase Prepare Phase Timeout & Hysteresis Bounds Specification

## 1. Dynamic Timeout Adjustment
Static timeouts in distributed agent consensus cause spurious aborts during transient network latency spikes.

## 2. RTT-Adaptive Timeout Formulation
Coordinator tracks smoothed round-trip times ($SRTT$) to participant cohorts:

$$T_{\text{prepare}} = \max(T_{\min}, \; SRTT + 4 \cdot RTTVAR)$$

Where $T_{\min} = 1000\,\text{ms}$ and $T_{\max} = 5000\,\text{ms}$.

## 3. Consensus Invariant
If all participants respond `VOTE_COMMIT` within $T_{\text{prepare}}$, coordinator persists `GLOBAL_COMMIT` to WAL; otherwise issues `GLOBAL_ABORT`.
