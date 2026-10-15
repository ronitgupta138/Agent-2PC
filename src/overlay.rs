//! Linux Ephemeral OverlayFS Driver for zero-copy transparent command staging.
//!
//! Spawns commands inside an unprivileged user + mount namespace (`unshare -m -U`)
//! with an ephemeral OverlayFS mount. Mutations are captured directly in the upperdir
//! in kernel space, avoiding full workspace scans and providing sub-millisecond rollbacks.

use crate::coordinator::Coordinator;
use crate::domain::{ParticipantVote, ResourceIntent, TransactionId, TxError};
use crate::adapters::FsAdapter;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use walkdir::WalkDir;

/// Detailed report from an OverlayFS-backed command execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OverlayExecReport {
    pub tid: TransactionId,
    pub command: String,
    pub exit_code: i32,
    pub staged_files: Vec<PathBuf>,
    pub stdout: String,
    pub stderr: String,
    pub upperdir_bytes: u64,
}

/// Runner that leverages Linux unprivileged user + mount namespaces for OverlayFS isolation.
pub struct OverlayRunner;

impl OverlayRunner {
    /// Check whether the host kernel supports unprivileged user namespaces and overlay mounts.
    pub fn is_supported() -> bool {
        let test_cmd = Command::new("unshare")
            .args(["--user", "--map-root-user", "--mount", "true"])
            .output();
        match test_cmd {
            Ok(output) => output.status.success(),
            Err(_) => false,
        }
    }

    /// Execute a command on top of an OverlayFS mount and auto-stage upperdir mutations.
    pub fn execute_and_stage(
        coord: &mut Coordinator,
        tid: &TransactionId,
        lower_dir: impl AsRef<Path>,
        command: &[String],
    ) -> Result<OverlayExecReport, TxError> {
        if command.is_empty() {
            return Err(TxError::Other("Empty command passed to overlay runner".into()));
        }

        let lower_dir = lower_dir.as_ref().to_path_buf().canonicalize()?;
        let cmd_str = command.join(" ");

        let tx_scratch = coord.base_dir().join("scratch").join(tid.to_string()).join("overlay");
        let upper_dir = tx_scratch.join("upper");
        let work_dir = tx_scratch.join("work");
        let merged_dir = tx_scratch.join("merged");

        fs::create_dir_all(&upper_dir)?;
        fs::create_dir_all(&work_dir)?;
        fs::create_dir_all(&merged_dir)?;

        let inner_runner = tx_scratch.join("run.sh");
        fs::write(&inner_runner, format!("#!/bin/sh\n{}\n", cmd_str))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&inner_runner, fs::Permissions::from_mode(0o755));
        }

        let script = format!(
            "mount -t overlay overlay -o lowerdir=\"{}\",upperdir=\"{}\",workdir=\"{}\" \"{}\" && cd \"{}\" && \"{}\"; sync",
            lower_dir.display(),
            upper_dir.display(),
            work_dir.display(),
            merged_dir.display(),
            merged_dir.display(),
            inner_runner.display()
        );

        let output = Command::new("unshare")
            .args(["--user", "--map-root-user", "--mount", "sh", "-c", &script])
            .output()?;

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let exit_code = output.status.code().unwrap_or(-1);

        if !output.status.success() {
            let _ = fs::remove_dir_all(&tx_scratch);
            return Ok(OverlayExecReport {
                tid: *tid,
                command: cmd_str,
                exit_code,
                staged_files: Vec::new(),
                stdout,
                stderr,
                upperdir_bytes: 0,
            });
        }

        // Discover modified and created files directly from upperdir
        let mut staged_files = Vec::new();
        let mut upperdir_bytes = 0u64;

        for entry in WalkDir::new(&upper_dir).into_iter().filter_map(|e| e.ok()) {
            let path = entry.path();
            if path.is_file() {
                if let Ok(meta) = fs::metadata(path) {
                    upperdir_bytes += meta.len();
                }

                let rel_path = match path.strip_prefix(&upper_dir) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let target_path = lower_dir.join(rel_path);

                if let Ok(content) = fs::read(path)
                    && let Ok(content_str) = String::from_utf8(content) {
                        let pid = format!(
                            "fs_overlay_{}",
                            target_path.file_name().unwrap_or_default().to_string_lossy()
                        );
                        let mut adapter = FsAdapter::new(pid);
                        let intent = ResourceIntent::FsWrite {
                            path: target_path.clone(),
                            content: content_str,
                            create_dirs: true,
                        };

                        let vote = coord.prepare_participant(tid, &mut adapter, intent)?;
                        if let ParticipantVote::Prepared(_) = vote {
                            staged_files.push(target_path);
                        }
                }
        }
    }

        // Clean up temporary overlay scratch mount (leave upper/work removed, keep staged artifacts safe)
        let _ = fs::remove_dir_all(&tx_scratch);

        Ok(OverlayExecReport {
            tid: *tid,
            command: cmd_str,
            exit_code,
            staged_files,
            stdout,
            stderr,
            upperdir_bytes,
        })
    }
}
