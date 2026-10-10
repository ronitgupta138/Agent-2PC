# Contributing to `agent-2pc`

Thank you for your interest in contributing to `agent-2pc`! We welcome contributions to core adapters, WAL storage primitives, MCP protocols, and test suites.

---

## 🛠️ Development Setup

### Prerequisites
- **Rust Toolchain:** 1.80+ (edition 2024 / 2021)
- **Cargo:** Included with Rust

### Build & Test
```bash
# Clone the repository
git clone https://github.com/ronitgupta138/agent-2pc.git
cd agent-2pc

# Run the test suite
cargo test

# Check code formatting and lints
cargo fmt --check
cargo clippy --all-targets -- -D warnings
```

---

## 📐 Architecture Guidelines

1. **Deterministic Rollback Invariant:** Any new `ResourceManager` adapter added to `src/adapters.rs` must guarantee that if `abort()` is invoked, all filesystem, process, or network artifacts staged during `prepare()` are 100% neutralized without requiring prompt-level LLM intervention.
2. **Crash Consistency:** Never bypass the Write-Ahead Log (`WalWriter`). State transitions (`BEGIN`, `ENLIST`, `PREPARED`, `COMMIT_DECISION`, `COMMITTED`, `ABORT_DECISION`, `ABORTED`) must be logged with CRC32 checksums and fsynced.
3. **Zero Leaked Scratches:** Staged state inside `.agent-tx/scratch/<tid>` must be purged on terminal state (`Committed` or `Aborted`).

---

## 🚀 Pull Request Checklist

- [ ] All unit and integration tests pass (`cargo test`).
- [ ] No compiler warnings or clippy warnings (`cargo clippy`).
- [ ] Added comprehensive unit or integration tests for new features.
- [ ] Clean commit messages following Conventional Commits (`feat:`, `fix:`, `docs:`, `test:`).
- [ ] Zero hardcoded local environment paths.
