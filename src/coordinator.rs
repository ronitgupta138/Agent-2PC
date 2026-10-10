//! Two-Phase Commit Transaction Coordinator for AI Agent Workflows.

use crate::adapters::ResourceManager;
use crate::domain::{
    ParticipantId, ParticipantVote, ResourceIntent, StageReceipt, TransactionId, TxError, TxState,
};
use crate::wal::{WalPayload, WalScanner, WalWriter};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

/// In-memory state of an active transaction.
#[derive(Debug, Clone)]
pub struct ActiveTx {
    pub tid: TransactionId,
    pub state: TxState,
    pub created_at: DateTime<Utc>,
    pub timeout_ms: u64,
    pub participants: Vec<ParticipantId>,
    pub receipts: HashMap<ParticipantId, StageReceipt>,
    pub abort_reason: Option<String>,
}

/// Summary report after crash recovery scan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryReport {
    pub total_records_scanned: usize,
    pub active_recovered: Vec<TransactionId>,
    pub rolled_back: Vec<TransactionId>,
    pub rolled_forward: Vec<TransactionId>,
}

/// Central Two-Phase Commit Coordinator.
pub struct Coordinator {
    base_dir: PathBuf,
    scratch_dir: PathBuf,
    wal_path: PathBuf,
    wal: WalWriter,
    transactions: HashMap<TransactionId, ActiveTx>,
}

impl Coordinator {
    /// Initialize coordinator in the specified data directory (e.g. `.agent-tx`).
    pub fn new(base_dir: impl AsRef<Path>) -> Result<Self, TxError> {
        let base_dir = base_dir.as_ref().to_path_buf();
        let scratch_dir = base_dir.join("scratch");
        let wal_path = base_dir.join("wal.bin");

        fs::create_dir_all(&base_dir)?;
        fs::create_dir_all(&scratch_dir)?;

        let wal = WalWriter::open(&wal_path)?;

        let mut coord = Self {
            base_dir,
            scratch_dir,
            wal_path,
            wal,
            transactions: HashMap::new(),
        };

        // Populate in-memory state from existing WAL records
        coord.replay_wal_into_memory()?;

        Ok(coord)
    }

    /// Replay WAL into in-memory transaction table.
    fn replay_wal_into_memory(&mut self) -> Result<(), TxError> {
        let records = WalScanner::read_all(&self.wal_path)?;
        for record in records {
            match record.payload {
                WalPayload::Begin {
                    tid,
                    timestamp,
                    timeout_ms,
                } => {
                    self.transactions.insert(
                        tid,
                        ActiveTx {
                            tid,
                            state: TxState::Active,
                            created_at: timestamp,
                            timeout_ms,
                            participants: Vec::new(),
                            receipts: HashMap::new(),
                            abort_reason: None,
                        },
                    );
                }
                WalPayload::Enlist {
                    tid,
                    participant_id,
                    ..
                } => {
                    if let Some(tx) = self.transactions.get_mut(&tid) {
                        if !tx.participants.contains(&participant_id) {
                            tx.participants.push(participant_id);
                            tx.state = TxState::Preparing;
                        }
                    }
                }
                WalPayload::Prepared {
                    tid,
                    participant_id,
                    receipt,
                } => {
                    if let Some(tx) = self.transactions.get_mut(&tid) {
                        tx.receipts.insert(participant_id, receipt);
                        if tx.receipts.len() == tx.participants.len() && !tx.participants.is_empty() {
                            tx.state = TxState::Prepared;
                        }
                    }
                }
                WalPayload::VoteAbort {
                    tid,
                    participant_id: _,
                    reason,
                } => {
                    if let Some(tx) = self.transactions.get_mut(&tid) {
                        tx.state = TxState::Aborting;
                        tx.abort_reason = Some(reason);
                    }
                }
                WalPayload::CommitDecision { tid, .. } => {
                    if let Some(tx) = self.transactions.get_mut(&tid) {
                        tx.state = TxState::Committing;
                    }
                }
                WalPayload::Committed { tid, .. } => {
                    if let Some(tx) = self.transactions.get_mut(&tid) {
                        tx.state = TxState::Committed;
                    }
                }
                WalPayload::AbortDecision { tid, reason, .. } => {
                    if let Some(tx) = self.transactions.get_mut(&tid) {
                        tx.state = TxState::Aborting;
                        tx.abort_reason = Some(reason);
                    }
                }
                WalPayload::Aborted { tid, .. } => {
                    if let Some(tx) = self.transactions.get_mut(&tid) {
                        tx.state = TxState::Aborted;
                    }
                }
                WalPayload::Checkpoint { .. } => {}
            }
        }
        Ok(())
    }

    /// Return base directory where WAL and scratch state reside.
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Begin a new transaction with optional timeout in milliseconds (default: 30000ms).
    pub fn begin(&mut self, timeout_ms: Option<u64>) -> Result<TransactionId, TxError> {
        let tid = TransactionId::new();
        let timeout = timeout_ms.unwrap_or(30_000);
        let now = Utc::now();

        self.wal.append(&WalPayload::Begin {
            tid,
            timestamp: now,
            timeout_ms: timeout,
        })?;

        self.transactions.insert(
            tid,
            ActiveTx {
                tid,
                state: TxState::Active,
                created_at: now,
                timeout_ms: timeout,
                participants: Vec::new(),
                receipts: HashMap::new(),
                abort_reason: None,
            },
        );

        Ok(tid)
    }

    /// Enlist a participant and execute Phase 1 (PREPARE).
    pub fn prepare_participant(
        &mut self,
        tid: &TransactionId,
        resource: &mut dyn ResourceManager,
        intent: ResourceIntent,
    ) -> Result<ParticipantVote, TxError> {
        let tx = self
            .transactions
            .get_mut(tid)
            .ok_or(TxError::TransactionNotFound(*tid))?;

        if tx.state != TxState::Active && tx.state != TxState::Preparing && tx.state != TxState::Prepared {
            return Err(TxError::InvalidState(
                *tid,
                tx.state,
                "prepare_participant",
            ));
        }

        let pid = resource.id().clone();
        if !tx.participants.contains(&pid) {
            tx.participants.push(pid.clone());
            tx.state = TxState::Preparing;
        }

        // Log Enlistment
        self.wal.append(&WalPayload::Enlist {
            tid: *tid,
            participant_id: pid.clone(),
            intent: intent.clone(),
        })?;

        // Execute Prepare
        let vote = resource.prepare(tid, &intent, &self.scratch_dir)?;

        match &vote {
            ParticipantVote::Prepared(receipt) => {
                self.wal.append(&WalPayload::Prepared {
                    tid: *tid,
                    participant_id: pid.clone(),
                    receipt: receipt.clone(),
                })?;

                if let Some(t) = self.transactions.get_mut(tid) {
                    t.receipts.insert(pid, receipt.clone());
                    if t.receipts.len() == t.participants.len() {
                        t.state = TxState::Prepared;
                    }
                }
            }
            ParticipantVote::Aborted { reason } => {
                self.wal.append(&WalPayload::VoteAbort {
                    tid: *tid,
                    participant_id: pid,
                    reason: reason.clone(),
                })?;

                if let Some(t) = self.transactions.get_mut(tid) {
                    t.state = TxState::Aborting;
                    t.abort_reason = Some(reason.clone());
                }
            }
        }

        Ok(vote)
    }

    /// Phase 2: COMMIT. Globally commits all prepared participants atomically.
    pub fn commit(
        &mut self,
        tid: &TransactionId,
        resources: &mut [Box<dyn ResourceManager>],
    ) -> Result<(), TxError> {
        let tx = self
            .transactions
            .get(tid)
            .ok_or(TxError::TransactionNotFound(*tid))?
            .clone();

        // Check if all participants voted PREPARED
        if tx.receipts.len() != tx.participants.len() || tx.participants.is_empty() {
            // Cannot commit: missing votes -> must trigger ABORT!
            let reason = format!(
                "Cannot commit: only {}/{} participants prepared",
                tx.receipts.len(),
                tx.participants.len()
            );
            self.abort(tid, &reason, resources)?;
            return Err(TxError::CommitFailed(
                ParticipantId::new("coordinator"),
                reason,
            ));
        }

        // POINT OF NO RETURN: Write CommitDecision to WAL
        let now = Utc::now();
        self.wal.append(&WalPayload::CommitDecision {
            tid: *tid,
            timestamp: now,
        })?;

        if let Some(t) = self.transactions.get_mut(tid) {
            t.state = TxState::Committing;
        }

        // Execute commit on each participant
        for res in resources.iter_mut() {
            let pid = res.id().clone();
            if let Some(receipt) = tx.receipts.get(&pid) {
                res.commit(tid, receipt)?;
                self.wal.append(&WalPayload::Committed {
                    tid: *tid,
                    participant_id: pid,
                })?;
            }
        }

        // Clean up transaction scratch folder
        let tx_scratch = self.scratch_dir.join(tid.to_string());
        let _ = fs::remove_dir_all(&tx_scratch);

        if let Some(t) = self.transactions.get_mut(tid) {
            t.state = TxState::Committed;
        }

        Ok(())
    }

    /// Phase 2: ABORT. Globally rolls back all participants and cleans up staging.
    pub fn abort(
        &mut self,
        tid: &TransactionId,
        reason: &str,
        resources: &mut [Box<dyn ResourceManager>],
    ) -> Result<(), TxError> {
        let tx = self
            .transactions
            .get(tid)
            .ok_or(TxError::TransactionNotFound(*tid))?
            .clone();

        // Write AbortDecision to WAL
        let now = Utc::now();
        self.wal.append(&WalPayload::AbortDecision {
            tid: *tid,
            reason: reason.to_string(),
            timestamp: now,
        })?;

        if let Some(t) = self.transactions.get_mut(tid) {
            t.state = TxState::Aborting;
            t.abort_reason = Some(reason.to_string());
        }

        // Roll back each participant
        for res in resources.iter_mut() {
            let pid = res.id().clone();
            let receipt = tx.receipts.get(&pid);
            res.abort(tid, receipt)?;
            self.wal.append(&WalPayload::Aborted {
                tid: *tid,
                participant_id: pid,
            })?;
        }

        // Clean up transaction scratch folder
        let tx_scratch = self.scratch_dir.join(tid.to_string());
        let _ = fs::remove_dir_all(&tx_scratch);

        if let Some(t) = self.transactions.get_mut(tid) {
            t.state = TxState::Aborted;
        }

        Ok(())
    }

    /// Return status of a specific transaction.
    pub fn get_status(&self, tid: &TransactionId) -> Option<&ActiveTx> {
        self.transactions.get(tid)
    }

    /// Return all active or uncommitted transactions.
    pub fn active_transactions(&self) -> Vec<&ActiveTx> {
        self.transactions
            .values()
            .filter(|t| t.state != TxState::Committed && t.state != TxState::Aborted)
            .collect()
    }

    /// Crash Recovery: Replays WAL to resolve any torn or dangling transactions.
    pub fn recover(
        &mut self,
        resources: &mut [Box<dyn ResourceManager>],
    ) -> Result<RecoveryReport, TxError> {
        let records = WalScanner::read_all(&self.wal_path)?;
        let mut rolled_back = Vec::new();
        let mut rolled_forward = Vec::new();

        // Collect transactions requiring resolution
        let dangling_tids: Vec<(TransactionId, TxState)> = self
            .transactions
            .iter()
            .filter(|(_, tx)| tx.state != TxState::Committed && tx.state != TxState::Aborted)
            .map(|(tid, tx)| (*tid, tx.state))
            .collect();

        for (tid, state) in dangling_tids {
            match state {
                TxState::Committing => {
                    // CommitDecision was logged before crash -> must roll forward!
                    let _ = self.commit(&tid, resources);
                    rolled_forward.push(tid);
                }
                TxState::Active | TxState::Preparing | TxState::Prepared | TxState::Aborting => {
                    // No CommitDecision recorded -> must roll back cleanly!
                    let _ = self.abort(&tid, "Crash recovery rollback", resources);
                    rolled_back.push(tid);
                }
                _ => {}
            }
        }

        let active_recovered: Vec<TransactionId> = self
            .transactions
            .keys()
            .cloned()
            .collect();

        Ok(RecoveryReport {
            total_records_scanned: records.len(),
            active_recovered,
            rolled_back,
            rolled_forward,
        })
    }
}
