//! Nested Savepoints & Speculative Execution Engine for agent-2pc.
//!
//! Provides transactional checkpointing inside an active 2PC transaction, allowing
//! autonomous AI agents to explore speculative modifications, run verification commands,
//! and deterministically unwind failed steps back to a named savepoint without
//! abandoning earlier verified stages.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Snapshot boundary representing a transactional savepoint within an active transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Savepoint {
    /// Identifier name for the savepoint (e.g. "pre_migration", "attempt_1")
    pub name: String,
    /// Number of participants registered when the savepoint was created
    pub participant_count: usize,
    /// Number of receipts captured when the savepoint was created
    pub receipt_count: usize,
    /// Timestamp when savepoint was marked
    pub created_at: DateTime<Utc>,
}

impl Savepoint {
    /// Create a new savepoint boundary with current UTC timestamp.
    pub fn new(name: impl Into<String>, participant_count: usize, receipt_count: usize) -> Self {
        Self {
            name: name.into(),
            participant_count,
            receipt_count,
            created_at: Utc::now(),
        }
    }
}
