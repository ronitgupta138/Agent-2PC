# Dynamic Cohort Registration & Discovery Protocol

## 1. Handshake Protocol
- Cohort registers with coordinator over Unix domain socket:
  `COHORT_REGISTER(service_name, tool_capabilities, rollback_support_level)`
- Coordinator acknowledges with leased participant ID and keepalive heartbeat interval.
