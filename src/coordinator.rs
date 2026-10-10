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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// In-memory state of an active transaction.
#[derive(Debug, Clone)]
pub struct ActiveTx {
    pub tid: TransactionId,
    pub state: TxState,
    pub created_at: DateTime<Utc>,
    pub timeout_ms: u64,
    pub participants: Vec<ParticipantId>,
    pub receipts: HashMap<ParticipantId, StageReceipt>,
    pub read_set: HashMap<PathBuf, u32>,
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
                            read_set: HashMap::new(),
                            abort_reason: None,
                        },
                    );
                }
                WalPayload::ReadSet { tid, path, hash } => {
                    if let Some(tx) = self.transactions.get_mut(&tid) {
                        tx.read_set.insert(path, hash);
                    }
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
                read_set: HashMap::new(),
                abort_reason: None,
            },
        );

        Ok(tid)
    }

    /// Record a file in the transaction's read-set for optimistic concurrency conflict detection.
    pub fn record_read(
        &mut self,
        tid: &TransactionId,
        path: impl AsRef<Path>,
    ) -> Result<u32, TxError> {
        let path = path.as_ref().to_path_buf();
        let bytes = fs::read(&path)?;
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&bytes);
        let hash = hasher.finalize();

        self.wal.append(&WalPayload::ReadSet {
            tid: *tid,
            path: path.clone(),
            hash,
        })?;

        let tx = self
            .transactions
            .get_mut(tid)
            .ok_or(TxError::TransactionNotFound(*tid))?;
        tx.read_set.insert(path, hash);

        Ok(hash)
    }

    /// Validate that all files in read-set have not been modified concurrently.
    pub fn validate_read_set(&self, tid: &TransactionId) -> Result<(), TxError> {
        let tx = self
            .transactions
            .get(tid)
            .ok_or(TxError::TransactionNotFound(*tid))?;

        for (path, &expected_hash) in &tx.read_set {
            if !path.exists() {
                return Err(TxError::Other(format!(
                    "Conflict detected: file {} was deleted during transaction",
                    path.display()
                )));
            }
            let bytes = fs::read(path)?;
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&bytes);
            let current_hash = hasher.finalize();

            if current_hash != expected_hash {
                return Err(TxError::Other(format!(
                    "Conflict detected: file {} was modified concurrently (expected hash 0x{:08x}, got 0x{:08x})",
                    path.display(),
                    expected_hash,
                    current_hash
                )));
            }
        }
        Ok(())
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

        // Validate Optimistic Concurrency Read-Set invariants
        if let Err(e) = self.validate_read_set(tid) {
            let reason = e.to_string();
            self.wal.append(&WalPayload::VoteAbort {
                tid: *tid,
                participant_id: pid,
                reason: reason.clone(),
            })?;
            if let Some(t) = self.transactions.get_mut(tid) {
                t.state = TxState::Aborting;
                t.abort_reason = Some(reason.clone());
            }
            return Ok(ParticipantVote::Aborted { reason });
        }

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

    /// Parallel Phase 1: Fan-out prepare to multiple resource managers concurrently with atomic short-circuit abort.
    pub fn prepare_batch_parallel(
        &mut self,
        tid: &TransactionId,
        items: &mut [(Box<dyn ResourceManager>, ResourceIntent)],
    ) -> Result<Vec<ParticipantVote>, TxError> {
        let tx = self
            .transactions
            .get_mut(tid)
            .ok_or(TxError::TransactionNotFound(*tid))?;

        if tx.state != TxState::Active && tx.state != TxState::Preparing && tx.state != TxState::Prepared {
            return Err(TxError::InvalidState(
                *tid,
                tx.state,
                "prepare_batch_parallel",
            ));
        }

        // Register all participants in WAL and memory upfront
        for (res, intent) in items.iter() {
            let pid = res.id().clone();
            if !tx.participants.contains(&pid) {
                tx.participants.push(pid.clone());
            }
            self.wal.append(&WalPayload::Enlist {
                tid: *tid,
                participant_id: pid,
                intent: intent.clone(),
            })?;
        }

        let short_circuit = Arc::new(AtomicBool::new(false));
        let scratch_dir = self.scratch_dir.clone();

        // Concurrently run prepare across scoped threads
        let votes: Vec<Result<ParticipantVote, TxError>> = std::thread::scope(|s| {
            let mut handles = Vec::with_capacity(items.len());

            for (res, intent) in items.iter_mut() {
                let sc = Arc::clone(&short_circuit);
                let s_dir = scratch_dir.clone();
                let handle = s.spawn(move || {
                    if sc.load(Ordering::Relaxed) {
                        return Ok(ParticipantVote::Aborted {
                            reason: "Short-circuited due to peer participant failure".into(),
                        });
                    }

                    let res_vote = res.prepare(tid, intent, &s_dir);
                    if let Ok(ParticipantVote::Aborted { .. }) = &res_vote {
                        sc.store(true, Ordering::SeqCst);
                    }
                    res_vote
                });
                handles.push(handle);
            }

            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        // Process collected votes and log to WAL
        let mut final_votes = Vec::with_capacity(votes.len());
        for (idx, vote_res) in votes.into_iter().enumerate() {
            let pid = items[idx].0.id().clone();
            let vote = vote_res?;

            match &vote {
                ParticipantVote::Prepared(receipt) => {
                    self.wal.append(&WalPayload::Prepared {
                        tid: *tid,
                        participant_id: pid.clone(),
                        receipt: receipt.clone(),
                    })?;

                    if let Some(t) = self.transactions.get_mut(tid) {
                        t.receipts.insert(pid, receipt.clone());
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
            final_votes.push(vote);
        }

        if let Some(t) = self.transactions.get_mut(tid) {
            if t.receipts.len() == t.participants.len() && t.state != TxState::Aborting {
                t.state = TxState::Prepared;
            }
        }

        Ok(final_votes)
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
