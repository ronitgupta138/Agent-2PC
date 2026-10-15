//! WAL Checkpoint Compaction Engine for agent-2pc.
//!
//! Compacts append-only write-ahead logs by discarding historical committed and aborted
//! transactions while preserving active, in-flight transactions, receipts, and OCC read-sets.

use crate::coordinator::ActiveTx;
use crate::domain::{TransactionId, TxError, TxState};
use crate::wal::{WalPayload, WalScanner, WalWriter};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Detailed metrics returned after a WAL compaction run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompactionReport {
    /// File size of wal.bin before compaction (bytes)
    pub original_bytes: u64,
    /// File size of wal.bin after compaction (bytes)
    pub compacted_bytes: u64,
    /// Total WAL frames in original log
    pub original_records: usize,
    /// Total WAL frames in compacted log
    pub compacted_records: usize,
    /// Number of active in-flight transactions preserved
    pub active_transactions: usize,
    /// Number of finalized (committed/aborted) transactions purged
    pub purged_transactions: usize,
}

/// Zero-downtime WAL compaction engine.
pub struct Compactor;

impl Compactor {
    /// Compact the WAL given an existing in-memory transactions map.
    pub fn compact(
        base_dir: impl AsRef<Path>,
        transactions: &HashMap<TransactionId, ActiveTx>,
    ) -> Result<CompactionReport, TxError> {
        let base_dir = base_dir.as_ref();
        let wal_path = base_dir.join("wal.bin");
        let tmp_wal_path = base_dir.join("wal.bin.tmp");

        let original_bytes = if wal_path.exists() {
            fs::metadata(&wal_path)?.len()
        } else {
            0
        };

        let records = if wal_path.exists() {
            WalScanner::read_all(&wal_path)?
        } else {
            Vec::new()
        };
        let original_records = records.len();

        // Identify in-flight transactions requiring preservation
        let active_entries: Vec<(&TransactionId, &ActiveTx)> = transactions
            .iter()
            .filter(|(_, tx)| tx.state != TxState::Committed && tx.state != TxState::Aborted)
            .collect();

        let active_tids: Vec<TransactionId> = active_entries.iter().map(|(tid, _)| **tid).collect();
        let active_count = active_entries.len();
        let purged_count = transactions.len().saturating_sub(active_count);

        // Open temporary WAL writer
        let mut tmp_writer = WalWriter::open(&tmp_wal_path)?;
        let mut compacted_records = 0;

        // 1. Emit Checkpoint barrier marker
        tmp_writer.append(&WalPayload::Checkpoint {
            active_tids: active_tids.clone(),
            timestamp: Utc::now(),
        })?;
        compacted_records += 1;

        // 2. Synthesize minimal records for each active transaction
        for (tid, tx) in &active_entries {
            // Begin record
            tmp_writer.append(&WalPayload::Begin {
                tid: **tid,
                timestamp: tx.created_at,
                timeout_ms: tx.timeout_ms,
            })?;
            compacted_records += 1;

            // Read-set fingerprints for OCC
            for (path, hash) in &tx.read_set {
                tmp_writer.append(&WalPayload::ReadSet {
                    tid: **tid,
                    path: path.clone(),
                    hash: *hash,
                })?;
                compacted_records += 1;
            }

            // Enlistments and Prepared receipts
            for pid in &tx.participants {
                if let Some(receipt) = tx.receipts.get(pid) {
                    tmp_writer.append(&WalPayload::Prepared {
                        tid: **tid,
                        participant_id: pid.clone(),
                        receipt: receipt.clone(),
                    })?;
                    compacted_records += 1;
                }
            }

            // In-flight state transitions
            if tx.state == TxState::Committing {
                tmp_writer.append(&WalPayload::CommitDecision {
                    tid: **tid,
                    timestamp: Utc::now(),
                })?;
                compacted_records += 1;
            } else if tx.state == TxState::Aborting {
                tmp_writer.append(&WalPayload::AbortDecision {
                    tid: **tid,
                    reason: tx
                        .abort_reason
                        .clone()
                        .unwrap_or_else(|| "Compacted in aborting state".into()),
                    timestamp: Utc::now(),
                })?;
                compacted_records += 1;
            }
        }

        // Flush and close tmp_writer
        drop(tmp_writer);

        let compacted_bytes = fs::metadata(&tmp_wal_path)?.len();

        // Atomically replace wal.bin with wal.bin.tmp
        fs::rename(&tmp_wal_path, &wal_path)?;

        Ok(CompactionReport {
            original_bytes,
            compacted_bytes,
            original_records,
            compacted_records,
            active_transactions: active_count,
            purged_transactions: purged_count,
        })
    }

    /// Compact the WAL offline by scanning the WAL from disk.
    pub fn compact_path(base_dir: impl AsRef<Path>) -> Result<CompactionReport, TxError> {
        let base_dir = base_dir.as_ref();
        let wal_path = base_dir.join("wal.bin");
        if !wal_path.exists() {
            return Ok(CompactionReport {
                original_bytes: 0,
                compacted_bytes: 0,
                original_records: 0,
                compacted_records: 0,
                active_transactions: 0,
                purged_transactions: 0,
            });
        }

        let coord = crate::coordinator::Coordinator::new(base_dir)?;
        let transactions = coord.active_transactions_map();
        Self::compact(base_dir, transactions)
    }
}
