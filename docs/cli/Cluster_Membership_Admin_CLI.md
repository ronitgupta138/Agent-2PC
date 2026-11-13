# Cluster Membership Administration & Node Join CLI Specification

## 1. CLI Commands
The `agent-2pc` command line interface provides administrative control over cluster topology:

```bash
agent-2pc cluster status                # Inspect active coordinator, quorum, and participants
agent-2pc cluster join --peer <address> # Add new agent participant node dynamically
agent-2pc cluster leave --force         # Graceful node decommission
```

## 2. Dynamic Membership Reconfiguration
Node joins and leaves execute as special configuration transactions committed via Two-Phase Commit to guarantee unanimous consensus on membership sets.
