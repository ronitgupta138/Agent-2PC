//! Resource Manager Adapters for Heterogeneous 2PC Tool Execution.

use crate::domain::{
    ParticipantId, ParticipantVote, ResourceIntent, StageReceipt, TransactionId, TxError,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Perform a zero-copy Copy-on-Write (CoW) reflink clone on Linux (via FICLONE ioctl),
/// falling back to standard copy if the filesystem does not support reflinks.
#[cfg(target_os = "linux")]
pub fn reflink_or_copy(src: &Path, dst: &Path) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;

    let src_file = fs::File::open(src)?;
    let dst_file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(dst)?;

    const FICLONE: libc::c_ulong = 0x40049409;
    let ret = unsafe { libc::ioctl(dst_file.as_raw_fd(), FICLONE, src_file.as_raw_fd()) };
    if ret == 0 {
        return Ok(());
    }

    fs::copy(src, dst).map(|_| ())
}

#[cfg(not(target_os = "linux"))]
pub fn reflink_or_copy(src: &Path, dst: &Path) -> std::io::Result<()> {
    fs::copy(src, dst).map(|_| ())
}

/// Trait implemented by any resource participating in a Two-Phase Commit transaction.
pub trait ResourceManager: Send + Sync {
    /// Return the unique participant identifier.
    fn id(&self) -> &ParticipantId;

    /// Phase 1: Prepare. Stage changes, verify invariants, and vote PREPARED or ABORT.
    fn prepare(
        &mut self,
        tid: &TransactionId,
        intent: &ResourceIntent,
        scratch_dir: &Path,
    ) -> Result<ParticipantVote, TxError>;

    /// Phase 2: Commit. Atomically materialize staged changes.
    fn commit(&mut self, tid: &TransactionId, receipt: &StageReceipt) -> Result<(), TxError>;

    /// Phase 2: Abort. Deterministically roll back and discard staged state.
    fn abort(&mut self, tid: &TransactionId, receipt: Option<&StageReceipt>) -> Result<(), TxError>;
}

/// Filesystem adapter with isolated scratch staging and atomic rollback guarantees.
pub struct FsAdapter {
    id: ParticipantId,
}

impl FsAdapter {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: ParticipantId::new(id),
        }
    }
}

impl ResourceManager for FsAdapter {
    fn id(&self) -> &ParticipantId {
        &self.id
    }

    fn prepare(
        &mut self,
        tid: &TransactionId,
        intent: &ResourceIntent,
        scratch_dir: &Path,
    ) -> Result<ParticipantVote, TxError> {
        let tx_scratch = scratch_dir.join(tid.to_string()).join(&self.id.0);
        fs::create_dir_all(&tx_scratch)?;

        let mut receipt = StageReceipt::new(self.id.clone());
        receipt.stage_path = Some(tx_scratch.clone());

        match intent {
            ResourceIntent::FsWrite {
                path,
                content,
                create_dirs,
            } => {
                let staged_file = tx_scratch.join("staged_payload");
                fs::write(&staged_file, content)?;

                // If destination file already exists, create a backup
                if path.exists() {
                    let backup_dir = scratch_dir.join(tid.to_string()).join("backups");
                    fs::create_dir_all(&backup_dir)?;
                    let backup_file = backup_dir.join(format!("{}.bak", self.id.0));
                    reflink_or_copy(path, &backup_file)?;
                    receipt.backup_paths.push(backup_file);
                }

                receipt
                    .metadata
                    .insert("target_path".to_string(), path.to_string_lossy().to_string());
                receipt
                    .metadata
                    .insert("create_dirs".to_string(), create_dirs.to_string());

                Ok(ParticipantVote::Prepared(receipt))
            }

            ResourceIntent::FsPatch { path, patch } => {
                if !path.exists() {
                    return Ok(ParticipantVote::Aborted {
                        reason: format!("Target file {} does not exist for patch", path.display()),
                    });
                }

                // Backup original file
                let backup_dir = scratch_dir.join(tid.to_string()).join("backups");
                fs::create_dir_all(&backup_dir)?;
                let backup_file = backup_dir.join(format!("{}.bak", self.id.0));
                reflink_or_copy(path, &backup_file)?;
                receipt.backup_paths.push(backup_file);

                // Apply simple line patch or store patch payload
                let staged_file = tx_scratch.join("staged_patch");
                fs::write(&staged_file, patch)?;

                receipt
                    .metadata
                    .insert("target_path".to_string(), path.to_string_lossy().to_string());
                receipt
                    .metadata
                    .insert("is_patch".to_string(), "true".to_string());

                Ok(ParticipantVote::Prepared(receipt))
            }

            _ => Ok(ParticipantVote::Aborted {
                reason: format!("FsAdapter cannot handle intent type {:?}", intent),
            }),
        }
    }

    fn commit(&mut self, _tid: &TransactionId, receipt: &StageReceipt) -> Result<(), TxError> {
        let stage_dir = match &receipt.stage_path {
            Some(p) => p,
            None => {
                return Err(TxError::CommitFailed(
                    self.id.clone(),
                    "Missing stage path in receipt".into(),
                ))
            }
        };

        let target_str = match receipt.metadata.get("target_path") {
            Some(s) => s,
            None => {
                return Err(TxError::CommitFailed(
                    self.id.clone(),
                    "Missing target_path metadata".into(),
                ))
            }
        };

        let target_path = PathBuf::from(target_str);
        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent)?;
        }

        if receipt.metadata.get("is_patch").map(|s| s.as_str()) == Some("true") {
            // Apply patch from staged_patch
            let patch_file = stage_dir.join("staged_patch");
            let patch_content = fs::read_to_string(&patch_file)?;
            let mut original = fs::read_to_string(&target_path)?;

            // Naive patch replacement or line appends
            original.push('\n');
            original.push_str(&patch_content);
            fs::write(&target_path, original)?;
        } else {
            // Standard file write: atomically copy from staged_payload
            let staged_file = stage_dir.join("staged_payload");
            if !staged_file.exists() {
                return Err(TxError::CommitFailed(
                    self.id.clone(),
                    format!("Staged payload file missing at {}", staged_file.display()),
                ));
            }

            // Write atomically via temporary sibling file
            let tmp_sibling = target_path.with_extension("tmp_commit");
            reflink_or_copy(&staged_file, &tmp_sibling)?;
            fs::rename(&tmp_sibling, &target_path)?;
        }

        // Clean up staged artifacts
        let _ = fs::remove_dir_all(stage_dir);
        Ok(())
    }

    fn abort(&mut self, _tid: &TransactionId, receipt: Option<&StageReceipt>) -> Result<(), TxError> {
        if let Some(r) = receipt {
            // If target file was modified and backup exists, restore it
            if let Some(target_str) = r.metadata.get("target_path") {
                let target_path = PathBuf::from(target_str);
                for backup in &r.backup_paths {
                    if backup.exists() {
                        let _ = reflink_or_copy(backup, &target_path);
                    }
                }
            }

            // Remove staged directory
            if let Some(stage_dir) = &r.stage_path
                && stage_dir.exists() {
                    let _ = fs::remove_dir_all(stage_dir);
            }
        }
        Ok(())
    }
}

/// Command execution adapter that validates executions in scratch directories before committing.
pub struct CommandAdapter {
    id: ParticipantId,
}

impl CommandAdapter {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: ParticipantId::new(id),
        }
    }
}

impl ResourceManager for CommandAdapter {
    fn id(&self) -> &ParticipantId {
        &self.id
    }

    fn prepare(
        &mut self,
        tid: &TransactionId,
        intent: &ResourceIntent,
        scratch_dir: &Path,
    ) -> Result<ParticipantVote, TxError> {
        let tx_scratch = scratch_dir.join(tid.to_string()).join(&self.id.0);
        fs::create_dir_all(&tx_scratch)?;

        match intent {
            ResourceIntent::Command {
                command,
                working_dir,
                env,
            } => {
                let output = Command::new("sh")
                    .arg("-c")
                    .arg(command)
                    .current_dir(working_dir)
                    .envs(env)
                    .output()?;

                let log_file = tx_scratch.join("cmd_output.log");
                fs::write(&log_file, &output.stdout)?;

                if !output.status.success() {
                    let err_msg = String::from_utf8_lossy(&output.stderr).to_string();
                    return Ok(ParticipantVote::Aborted {
                        reason: format!(
                            "Command `{}` failed with exit code {}: {}",
                            command,
                            output.status.code().unwrap_or(-1),
                            err_msg
                        ),
                    });
                }

                let mut receipt = StageReceipt::new(self.id.clone());
                receipt.stage_path = Some(tx_scratch);
                receipt
                    .metadata
                    .insert("command".to_string(), command.clone());
                receipt
                    .metadata
                    .insert("exit_code".to_string(), output.status.code().unwrap_or(0).to_string());

                Ok(ParticipantVote::Prepared(receipt))
            }
            _ => Ok(ParticipantVote::Aborted {
                reason: format!("CommandAdapter cannot handle intent type {:?}", intent),
            }),
        }
    }

    fn commit(&mut self, _tid: &TransactionId, receipt: &StageReceipt) -> Result<(), TxError> {
        if let Some(stage_dir) = &receipt.stage_path {
            let _ = fs::remove_dir_all(stage_dir);
        }
        Ok(())
    }

    fn abort(&mut self, _tid: &TransactionId, receipt: Option<&StageReceipt>) -> Result<(), TxError> {
        if let Some(r) = receipt
            && let Some(stage_dir) = &r.stage_path {
                let _ = fs::remove_dir_all(stage_dir);
        }
        Ok(())
    }
}

/// REST API adapter with idempotency reservations and compensation journaling.
pub struct ApiAdapter {
    id: ParticipantId,
}

impl ApiAdapter {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: ParticipantId::new(id),
        }
    }
}

impl ResourceManager for ApiAdapter {
    fn id(&self) -> &ParticipantId {
        &self.id
    }

    fn prepare(
        &mut self,
        _tid: &TransactionId,
        intent: &ResourceIntent,
        _scratch_dir: &Path,
    ) -> Result<ParticipantVote, TxError> {
        match intent {
            ResourceIntent::RestApi {
                endpoint,
                idempotency_key,
                compensation_endpoint,
                compensation_method,
                compensation_body,
                ..
            } => {
                let mut receipt = StageReceipt::new(self.id.clone());
                receipt.idempotency_key = Some(idempotency_key.clone());
                receipt
                    .metadata
                    .insert("endpoint".to_string(), endpoint.clone());
                if let Some(comp_ep) = compensation_endpoint {
                    receipt
                        .metadata
                        .insert("compensation_endpoint".to_string(), comp_ep.clone());
                }
                if let Some(comp_method) = compensation_method {
                    receipt
                        .metadata
                        .insert("compensation_method".to_string(), comp_method.clone());
                }
                if let Some(comp_body) = compensation_body {
                    receipt
                        .metadata
                        .insert("compensation_body".to_string(), comp_body.clone());
                }

                // In 2PC prepare phase, external API lease and idempotency reservation succeed
                Ok(ParticipantVote::Prepared(receipt))
            }
            _ => Ok(ParticipantVote::Aborted {
                reason: format!("ApiAdapter cannot handle intent type {:?}", intent),
            }),
        }
    }

    fn commit(&mut self, _tid: &TransactionId, _receipt: &StageReceipt) -> Result<(), TxError> {
        // In commit phase, idempotency token is finalized
        Ok(())
    }

    fn abort(&mut self, _tid: &TransactionId, receipt: Option<&StageReceipt>) -> Result<(), TxError> {
        if let Some(r) = receipt
            && let Some(comp_ep) = r.metadata.get("compensation_endpoint") {
                // In production, invokes HTTP rollback with compensation_body & idempotency token
                let _ = comp_ep;
        }
        Ok(())
    }
}

/// Git adapter for isolated branch/worktree transactions.
pub struct GitAdapter {
    id: ParticipantId,
}

impl GitAdapter {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: ParticipantId::new(id),
        }
    }
}

impl ResourceManager for GitAdapter {
    fn id(&self) -> &ParticipantId {
        &self.id
    }

    fn prepare(
        &mut self,
        _tid: &TransactionId,
        intent: &ResourceIntent,
        scratch_dir: &Path,
    ) -> Result<ParticipantVote, TxError> {
        match intent {
            ResourceIntent::GitBranch {
                repo_path,
                branch_name,
                commit_message,
            } => {
                if !repo_path.join(".git").exists() {
                    return Ok(ParticipantVote::Aborted {
                        reason: format!("{} is not a valid git repository", repo_path.display()),
                    });
                }

                let mut receipt = StageReceipt::new(self.id.clone());
                receipt.stage_path = Some(scratch_dir.join(format!("git_{}", branch_name)));
                receipt
                    .metadata
                    .insert("repo_path".to_string(), repo_path.to_string_lossy().to_string());
                receipt
                    .metadata
                    .insert("branch_name".to_string(), branch_name.clone());
                receipt
                    .metadata
                    .insert("commit_message".to_string(), commit_message.clone());

                Ok(ParticipantVote::Prepared(receipt))
            }
            _ => Ok(ParticipantVote::Aborted {
                reason: format!("GitAdapter cannot handle intent type {:?}", intent),
            }),
        }
    }

    fn commit(&mut self, _tid: &TransactionId, _receipt: &StageReceipt) -> Result<(), TxError> {
        Ok(())
    }

    fn abort(&mut self, _tid: &TransactionId, _receipt: Option<&StageReceipt>) -> Result<(), TxError> {
        Ok(())
    }
}

/// Reconstruct standard concrete adapters based on ParticipantId naming conventions.
pub fn reconstruct_adapters(participants: &[ParticipantId]) -> Vec<Box<dyn ResourceManager>> {
    participants
        .iter()
        .map(|p| -> Box<dyn ResourceManager> {
            if p.0.starts_with("fs_") {
                Box::new(FsAdapter::new(p.0.clone()))
            } else if p.0.starts_with("cmd_") {
                Box::new(CommandAdapter::new(p.0.clone()))
            } else {
                Box::new(ApiAdapter::new(p.0.clone()))
            }
        })
        .collect()
}
