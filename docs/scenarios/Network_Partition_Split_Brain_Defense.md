# Network Partition and Split-Brain Defense Mechanics

## 1. Fencing Tokens
- Every active coordinator session generates a monotonically increasing epoch fencing token.
- Tool servers reject write commands presenting a fencing token lower than the latest observed epoch.
