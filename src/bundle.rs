//! Transaction Export & Import Bundles for Multi-Agent Swarms.
//!
//! Enables serializing an active 2PC transaction into a portable `.a2pc` bundle
//! with CRC32 integrity verification, allowing cross-agent review and handoffs.

use crate::coordinator::{ActiveTx, Coordinator};
use crate::domain::{TransactionId, TxError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

const BUNDLE_MAGIC: &str = "A2PC_BUNDLE_V1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundlePayload {
    pub tid: TransactionId,
    pub tx_state: ActiveTx,
    pub scratch_files: BTreeMap<String, String>, // relative_path -> content
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionBundle {
    pub magic: String,
    pub crc32: u32,
    pub payload_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportReport {
    pub tid: TransactionId,
    pub output_path: PathBuf,
    pub participant_count: usize,
    pub file_count: usize,
    pub bytes_written: usize,
    pub crc32: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportReport {
    pub tid: TransactionId,
    pub source_path: PathBuf,
    pub participant_count: usize,
    pub restored_files: usize,
    pub status: String,
}

pub struct BundleManager;

impl BundleManager {
    /// Export an active transaction and all its staged artifacts to a bundle file.
    pub fn export_bundle(
        coord: &Coordinator,
        tid: &TransactionId,
        output_path: impl AsRef<Path>,
    ) -> Result<ExportReport, TxError> {
        let tx = coord
            .get_status(tid)
            .ok_or(TxError::TransactionNotFound(*tid))?;

        let mut scratch_files = BTreeMap::new();
        let tid_scratch = coord.scratch_dir().join(tid.to_string());

        if tid_scratch.exists() {
            for entry in walkdir::WalkDir::new(&tid_scratch)
                .into_iter()
                .filter_map(|e| e.ok())
            {
                if entry.file_type().is_file() {
                    let rel_path = entry
                        .path()
                        .strip_prefix(&tid_scratch)
                        .map_err(|e| TxError::Other(e.to_string()))?
                        .to_string_lossy()
                        .to_string();

                    let content = fs::read_to_string(entry.path())
                        .unwrap_or_else(|_| String::from_utf8_lossy(&fs::read(entry.path()).unwrap_or_default()).to_string());

                    scratch_files.insert(rel_path, content);
                }
            }
        }

        let file_count = scratch_files.len();
        let payload = BundlePayload {
            tid: *tid,
            tx_state: tx.clone(),
            scratch_files,
        };

        let payload_json = serde_json::to_string_pretty(&payload)?;

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(payload_json.as_bytes());
        let crc32 = hasher.finalize();

        let bundle = TransactionBundle {
            magic: BUNDLE_MAGIC.to_string(),
            crc32,
            payload_json,
        };

        let bundle_bytes = serde_json::to_string_pretty(&bundle)?;

        let out_path = output_path.as_ref().to_path_buf();
        if let Some(parent) = out_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&out_path, &bundle_bytes)?;

        Ok(ExportReport {
            tid: *tid,
            output_path: out_path,
            participant_count: tx.participants.len(),
            file_count,
            bytes_written: bundle_bytes.len(),
            crc32,
        })
    }

    /// Import a transaction bundle, verifying its CRC32 and materializing scratch state.
    pub fn import_bundle(
        coord: &mut Coordinator,
        bundle_path: impl AsRef<Path>,
    ) -> Result<ImportReport, TxError> {
        let b_path = bundle_path.as_ref().to_path_buf();
        let content = fs::read_to_string(&b_path)?;

        let bundle: TransactionBundle = serde_json::from_str(&content)
            .map_err(|e| TxError::Other(format!("Malformed bundle file: {}", e)))?;

        if bundle.magic != BUNDLE_MAGIC {
            return Err(TxError::Other(format!(
                "Invalid bundle magic header: expected {}, found {}",
                BUNDLE_MAGIC, bundle.magic
            )));
        }

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(bundle.payload_json.as_bytes());
        let expected_crc = hasher.finalize();

        if bundle.crc32 != expected_crc {
            return Err(TxError::ChecksumMismatch {
                expected: bundle.crc32,
                found: expected_crc,
            });
        }

        let payload: BundlePayload = serde_json::from_str(&bundle.payload_json)?;
        let tid = payload.tid;
        let dest_scratch = coord.scratch_dir().join(tid.to_string());
        fs::create_dir_all(&dest_scratch)?;

        // Restore scratch files
        let mut restored_count = 0;
        for (rel, data) in &payload.scratch_files {
            let file_dest = dest_scratch.join(rel);
            if let Some(parent) = file_dest.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&file_dest, data.as_bytes())?;
            restored_count += 1;
        }

        let tx = payload.tx_state;
        let part_count = tx.participants.len();
        coord.register_imported_transaction(tx)?;

        Ok(ImportReport {
            tid,
            source_path: b_path,
            participant_count: part_count,
            restored_files: restored_count,
            status: "PREPARED".to_string(),
        })
    }
}
