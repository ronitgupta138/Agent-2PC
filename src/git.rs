//! Git Pre-Commit Hook & Transaction Bridge for Version-Controlled Workspaces.
//!
//! Enforces repository integrity by blocking Git commits while in-flight or
//! uncommitted 2PC transactions exist. Provides atomic 2PC-to-Git commit bridging.

use crate::coordinator::Coordinator;
use crate::domain::{TransactionId, TxError, TxState};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookInstallReport {
    pub hook_path: PathBuf,
    pub installed: bool,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookCheckReport {
    pub clean: bool,
    pub in_flight_tids: Vec<TransactionId>,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitCommitReport {
    pub tid: TransactionId,
    pub commit_hash: String,
    pub files_committed: Vec<PathBuf>,
    pub message: String,
}

pub struct GitIntegrator;

impl GitIntegrator {
    /// Install an agent-2pc pre-commit hook into the target Git repository.
    pub fn install_hook(
        git_root: impl AsRef<Path>,
        tx_dir: impl AsRef<Path>,
    ) -> Result<HookInstallReport, TxError> {
        let root = git_root.as_ref();
        let hooks_dir = root.join(".git").join("hooks");

        if !hooks_dir.exists() {
            fs::create_dir_all(&hooks_dir)?;
        }

        let hook_path = hooks_dir.join("pre-commit");
        let tx_dir_canon = tx_dir.as_ref().to_string_lossy();

        let hook_content = format!(
            "#!/bin/sh\n\
             # agent-2pc repository integrity guard\n\
             agent-2pc hook check --tx-dir \"{}\" || exit 1\n",
            tx_dir_canon
        );

        fs::write(&hook_path, hook_content.as_bytes())?;

        #[cfg(unix)]
        {
            let mut perms = fs::metadata(&hook_path)?.permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&hook_path, perms)?;
        }

        Ok(HookInstallReport {
            hook_path,
            installed: true,
            message: "Successfully installed agent-2pc pre-commit hook.".to_string(),
        })
    }

    /// Check if the .agent-tx directory is clean of in-flight uncommitted transactions.
    pub fn check_invariants(tx_dir: impl AsRef<Path>) -> Result<HookCheckReport, TxError> {
        let tx_dir = tx_dir.as_ref();
        if !tx_dir.exists() {
            return Ok(HookCheckReport {
                clean: true,
                in_flight_tids: Vec::new(),
                message: "No .agent-tx directory found. Repository is clean.".to_string(),
            });
        }

        let coord = match Coordinator::new(tx_dir) {
            Ok(c) => c,
            Err(_) => {
                return Ok(HookCheckReport {
                    clean: true,
                    in_flight_tids: Vec::new(),
                    message: "Unable to read coordinator; assuming clean.".to_string(),
                });
            }
        };

        let in_flight: Vec<TransactionId> = coord
            .active_transactions()
            .iter()
            .filter(|tx| tx.state != TxState::Committed && tx.state != TxState::Aborted)
            .map(|tx| tx.tid)
            .collect();

        if in_flight.is_empty() {
            Ok(HookCheckReport {
                clean: true,
                in_flight_tids: Vec::new(),
                message: "Zero in-flight transactions. Git workspace is safe to commit.".to_string(),
            })
        } else {
            Ok(HookCheckReport {
                clean: false,
                in_flight_tids: in_flight.clone(),
                message: format!(
                    "Integrity violation: {} active/prepared 2PC transaction(s) pending. Commit or abort before git commit.",
                    in_flight.len()
                ),
            })
        }
    }

    /// Atomically commit 2PC transaction and immediately create a Git commit.
    pub fn git_commit(
        coord: &mut Coordinator,
        tid: &TransactionId,
        message: &str,
        working_dir: Option<&Path>,
    ) -> Result<GitCommitReport, TxError> {
        let tx = coord
            .get_status(tid)
            .ok_or(TxError::TransactionNotFound(*tid))?;

        let mut touched_files = Vec::new();
        for receipt in tx.receipts.values() {
            if let Some(target) = receipt.metadata.get("target_path") {
                let p = PathBuf::from(target);
                if !touched_files.contains(&p) {
                    touched_files.push(p);
                }
            }
        }

        // Reconstruct adapters and commit
        let mut adapters = crate::adapters::reconstruct_adapters(&tx.participants);
        coord.commit(tid, &mut adapters[..])?;

        let work_dir = working_dir.unwrap_or_else(|| Path::new("."));

        // Stage all touched files into Git index
        for f in &touched_files {
            let status = Command::new("git")
                .arg("add")
                .arg(f)
                .current_dir(work_dir)
                .status()
                .map_err(|e| TxError::Other(format!("Failed to run git add: {}", e)))?;

            if !status.success() {
                return Err(TxError::Other(format!(
                    "git add failed for {}",
                    f.display()
                )));
            }
        }

        // Run git commit
        let commit_output = Command::new("git")
            .arg("commit")
            .arg("-m")
            .arg(message)
            .current_dir(work_dir)
            .output()
            .map_err(|e| TxError::Other(format!("Failed to run git commit: {}", e)))?;

        if !commit_output.status.success() {
            let stderr = String::from_utf8_lossy(&commit_output.stderr);
            return Err(TxError::Other(format!("git commit failed: {}", stderr)));
        }

        // Retrieve commit hash
        let hash_output = Command::new("git")
            .arg("rev-parse")
            .arg("HEAD")
            .current_dir(work_dir)
            .output()
            .map_err(|e| TxError::Other(format!("Failed to run git rev-parse: {}", e)))?;

        let commit_hash = String::from_utf8_lossy(&hash_output.stdout)
            .trim()
            .to_string();

        Ok(GitCommitReport {
            tid: *tid,
            commit_hash,
            files_committed: touched_files,
            message: message.to_string(),
        })
    }
}
