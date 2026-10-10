//! agent-2pc: Heterogeneous Two-Phase Commit Coordinator for AI Agent Tool Execution.

pub mod domain;
pub mod wal;
pub mod adapters;
pub mod coordinator;
pub mod mcp;
pub mod monitor;
pub mod sandbox;
pub mod merge;
