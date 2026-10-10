//! Crash-Consistent Append-Only Write-Ahead Log (WAL) with CRC32 Verification.

use crate::domain::{ParticipantId, ResourceIntent, StageReceipt, TransactionId, TxError};
use chrono::{DateTime, Utc};
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub const WAL_MAGIC: [u8; 4] = [0x41, 0x32, 0x50, 0x43]; // "A2PC"
pub const WAL_VERSION: u8 = 1;

/// Distinct record types logged in the WAL.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "tag", content = "data", rename_all = "snake_case")]
pub enum WalPayload {
    /// Transaction started
    Begin {
        tid: TransactionId,
        timestamp: DateTime<Utc>,
        timeout_ms: u64,
    },
    /// Participant enlisted with intent
    Enlist {
        tid: TransactionId,
        participant_id: ParticipantId,
        intent: ResourceIntent,
    },
    /// Read-set fingerprint for optimistic concurrency conflict detection
    ReadSet {
        tid: TransactionId,
        path: PathBuf,
        hash: u32,
    },
    /// Participant voted PREPARED
    Prepared {
        tid: TransactionId,
        participant_id: ParticipantId,
        receipt: StageReceipt,
    },
    /// Participant voted ABORT
    VoteAbort {
        tid: TransactionId,
        participant_id: ParticipantId,
        reason: String,
    },
    /// Coordinator decided to globally COMMIT
    CommitDecision {
        tid: TransactionId,
        timestamp: DateTime<Utc>,
    },
    /// Participant successfully committed
    Committed {
        tid: TransactionId,
        participant_id: ParticipantId,
    },
    /// Coordinator decided to globally ABORT
    AbortDecision {
        tid: TransactionId,
        reason: String,
        timestamp: DateTime<Utc>,
    },
    /// Participant successfully aborted / compensated
    Aborted {
        tid: TransactionId,
        participant_id: ParticipantId,
    },
    /// Checkpoint marker
    Checkpoint {
        active_tids: Vec<TransactionId>,
        timestamp: DateTime<Utc>,
    },
}

/// Disk entry containing framing and CRC32 verification.
#[derive(Debug, Clone)]
pub struct WalRecord {
    pub offset: u64,
    pub payload: WalPayload,
    pub crc32: u32,
}

/// Durable Append-Only Write-Ahead Log Writer.
pub struct WalWriter {
    path: PathBuf,
    file: File,
}

impl WalWriter {
    /// Open or create the WAL file at the specified path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, TxError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        Ok(Self { path, file })
    }

    /// Append a payload to WAL, ensuring fsync durability and CRC32 verification.
    pub fn append(&mut self, payload: &WalPayload) -> Result<u64, TxError> {
        let serialized = serde_json::to_vec(payload)?;
        let mut hasher = Hasher::new();
        hasher.update(&serialized);
        let checksum = hasher.finalize();

        let current_offset = self.file.seek(SeekFrom::End(0))?;

        // Frame format:
        // [4 bytes Magic] [1 byte Version] [4 bytes Payload Len] [4 bytes CRC32] [N bytes Payload]
        self.file.write_all(&WAL_MAGIC)?;
        self.file.write_all(&[WAL_VERSION])?;
        self.file.write_all(&(serialized.len() as u32).to_le_bytes())?;
        self.file.write_all(&checksum.to_le_bytes())?;
        self.file.write_all(&serialized)?;

        // Ensure durable on disk
        self.file.sync_all()?;

        Ok(current_offset)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Scanner that reads and validates WAL entries from disk.
pub struct WalScanner;

impl WalScanner {
    /// Read and validate all valid records from a WAL file.
    pub fn read_all(path: impl AsRef<Path>) -> Result<Vec<WalRecord>, TxError> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut records = Vec::new();
        let mut offset = 0u64;

        loop {
            let mut magic = [0u8; 4];
            match reader.read_exact(&mut magic) {
                Ok(_) => {
                    if magic != WAL_MAGIC {
                        return Err(TxError::WalError(format!(
                            "Invalid WAL magic at offset {offset}: expected {:?}, got {:?}",
                            WAL_MAGIC, magic
                        )));
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Clean end of log
                    break;
                }
                Err(e) => return Err(TxError::Io(e)),
            }

            let mut version = [0u8; 1];
            reader.read_exact(&mut version)?;
            if version[0] != WAL_VERSION {
                return Err(TxError::WalError(format!(
                    "Unsupported WAL version {} at offset {}",
                    version[0], offset
                )));
            }

            let mut len_bytes = [0u8; 4];
            reader.read_exact(&mut len_bytes)?;
            let payload_len = u32::from_le_bytes(len_bytes) as usize;

            let mut crc_bytes = [0u8; 4];
            reader.read_exact(&mut crc_bytes)?;
            let expected_crc = u32::from_le_bytes(crc_bytes);

            let mut payload_bytes = vec![0u8; payload_len];
            match reader.read_exact(&mut payload_bytes) {
                Ok(_) => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Torn write at EOF; truncate/stop
                    break;
                }
                Err(e) => return Err(TxError::Io(e)),
            }

            // Verify checksum
            let mut hasher = Hasher::new();
            hasher.update(&payload_bytes);
            let found_crc = hasher.finalize();

            if expected_crc != found_crc {
                return Err(TxError::ChecksumMismatch {
                    expected: expected_crc,
                    found: found_crc,
                });
            }

            let payload: WalPayload = serde_json::from_slice(&payload_bytes)?;
            records.push(WalRecord {
                offset,
                payload,
                crc32: expected_crc,
            });

            // Frame size: 4 + 1 + 4 + 4 + payload_len = 13 + payload_len
            offset += 13 + payload_len as u64;
        }

        Ok(records)
    }
}
