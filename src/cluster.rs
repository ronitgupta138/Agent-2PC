//! Distributed Multi-Node 2PC Cluster Coordinator & Consensus Protocol.

use crate::domain::{ParticipantId, ParticipantVote, ResourceIntent, TransactionId, TxError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Wire packet exchanged across cluster nodes in distributed 2PC.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClusterMessage {
    /// Coordinator requests remote participant to prepare
    PrepareRequest {
        tid: TransactionId,
        participant_id: ParticipantId,
        intent: ResourceIntent,
    },
    /// Participant returns Phase 1 vote
    PrepareResponse {
        tid: TransactionId,
        participant_id: ParticipantId,
        vote: ParticipantVote,
    },
    /// Coordinator issues global Phase 2 COMMIT
    CommitRequest {
        tid: TransactionId,
    },
    /// Participant confirms commit completion
    CommitResponse {
        tid: TransactionId,
        participant_id: ParticipantId,
        success: bool,
    },
    /// Coordinator issues global Phase 2 ABORT
    AbortRequest {
        tid: TransactionId,
        reason: String,
    },
    /// Participant confirms rollback completion
    AbortResponse {
        tid: TransactionId,
        participant_id: ParticipantId,
        success: bool,
    },
}

/// Remote cluster participant endpoint tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNode {
    pub node_id: String,
    pub endpoint: String,
    pub active: bool,
}

/// Distributed consensus coordinator tracking remote node participants.
#[derive(Debug, Default)]
pub struct ClusterCoordinator {
    nodes: HashMap<String, ClusterNode>,
}

impl ClusterCoordinator {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }

    /// Register a remote participant node.
    pub fn register_node(&mut self, node_id: impl Into<String>, endpoint: impl Into<String>) {
        let id = node_id.into();
        self.nodes.insert(
            id.clone(),
            ClusterNode {
                node_id: id,
                endpoint: endpoint.into(),
                active: true,
            },
        );
    }

    /// Process a received remote vote and evaluate consensus.
    pub fn evaluate_votes(&self, votes: &[ParticipantVote]) -> Result<bool, TxError> {
        if votes.is_empty() {
            return Ok(false);
        }

        for vote in votes {
            if let ParticipantVote::Aborted { reason } = vote {
                return Err(TxError::Other(format!(
                    "Distributed 2PC consensus aborted by peer: {}",
                    reason
                )));
            }
        }

        Ok(true)
    }

    /// Generate distributed Phase 2 decision packet based on consensus evaluation.
    pub fn create_decision_packet(
        &self,
        tid: TransactionId,
        all_prepared: bool,
        abort_reason: Option<&str>,
    ) -> ClusterMessage {
        if all_prepared {
            ClusterMessage::CommitRequest { tid }
        } else {
            ClusterMessage::AbortRequest {
                tid,
                reason: abort_reason.unwrap_or("Consensus failure").to_string(),
            }
        }
    }
}
