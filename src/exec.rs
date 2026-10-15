//! Transparent Command Execution Staging Engine for agent-2pc.
//!
//! Executes build tools, compilers, migrations, and scripts in the workspace,
//! automatically discovering created and modified files, snapshotting backups,
//! and staging all mutations into Phase 1 2PC receipts with zero manual effort.

use crate::adapters::FsAdapter;
use crate::coordinator::Coordinator;
use crate::domain::{ParticipantVote, ResourceIntent, TransactionId, TxError};
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use walkdir::WalkDir;

/// Execution report capturing stdout, stderr, exit code, and auto-staged files.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecReport {
    pub tid: TransactionId,
    pub command: String,
    pub exit_code: i32,
    pub staged_files: Vec<PathBuf>,
    pub stdout: String,
    pub stderr: String,
}

/// Runner that captures workspace side-effects of external commands into 2PC Phase 1 staging.
pub struct ExecRunner;

impl ExecRunner {
    /// Execute command in working directory and capture all file side-effects into 2PC staging.
    pub fn execute_and_stage(
        coord: &mut Coordinator,
        tid: &TransactionId,
        working_dir: impl AsRef<Path>,
        command: &[String],
    ) -> Result<ExecReport, TxError> {
        if command.is_empty() {
            return Err(TxError::Other("Empty command passed to exec runner".into()));
        }

        let working_dir = working_dir.as_ref().to_path_buf();
        let cmd_str = command.join(" ");

        // 1. Snapshot directory file fingerprints and original contents before execution
        let before_map = Self::scan_fingerprints(&working_dir, coord.base_dir())?;

        // 2. Execute process
        let output = if command.len() == 1 {
            Command::new("sh")
                .arg("-c")
                .arg(&command[0])
                .current_dir(&working_dir)
                .output()?
        } else {
            Command::new(&command[0])
                .args(&command[1..])
                .current_dir(&working_dir)
                .output()?
        };

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let exit_code = output.status.code().unwrap_or(-1);

        // 3. Scan directory file fingerprints after execution
        let after_map = Self::scan_fingerprints(&working_dir, coord.base_dir())?;

        // If execution failed, revert any mutations immediately and return failure report
        if !output.status.success() {
            for path in after_map.keys() {
                if let Some((_, original_bytes)) = before_map.get(path) {
                    let _ = fs::write(path, original_bytes);
                } else {
                    let _ = fs::remove_file(path);
                }
            }
            return Ok(ExecReport {
                tid: *tid,
                command: cmd_str,
                exit_code,
                staged_files: Vec::new(),
                stdout,
                stderr,
            });
        }

        // 4. Identify modified and newly created files
        let mut mutated_files = Vec::new();
        for (path, (after_hash, _)) in &after_map {
            match before_map.get(path) {
                Some((before_hash, _)) => {
                    if before_hash != after_hash {
                        mutated_files.push((path.clone(), false));
                    }
                }
                None => {
                    mutated_files.push((path.clone(), true));
                }
            }
        }

        // 5. Read new contents and stage via FsAdapter under tid
        let mut staged_files = Vec::new();
        for (file_path, is_new) in mutated_files {
            if file_path.is_file() {
                let content = fs::read_to_string(&file_path).unwrap_or_default();
                let pid = format!(
                    "fs_exec_{}",
                    file_path
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                );

                if is_new {
                    // Remove from working tree before prepare so prepare takes clean new-file path
                    let _ = fs::remove_file(&file_path);

                    let mut adapter = FsAdapter::new(pid);
                    let intent = ResourceIntent::FsWrite {
                        path: file_path.clone(),
                        content,
                        create_dirs: true,
                    };

                    let vote = coord.prepare_participant(tid, &mut adapter, intent)?;
                    if let ParticipantVote::Prepared(_) = vote {
                        staged_files.push(file_path.clone());
                    }
                } else {
                    // Revert file to original before staging so FsAdapter captures clean backup
                    if let Some((_, original_bytes)) = before_map.get(&file_path) {
                        let _ = fs::write(&file_path, original_bytes);
                    }

                    let mut adapter = FsAdapter::new(pid);
                    let intent = ResourceIntent::FsWrite {
                        path: file_path.clone(),
                        content,
                        create_dirs: true,
                    };

                    let vote = coord.prepare_participant(tid, &mut adapter, intent)?;
                    if let ParticipantVote::Prepared(_) = vote {
                        staged_files.push(file_path.clone());
                    }
                }
            }
        }

        Ok(ExecReport {
            tid: *tid,
            command: cmd_str,
            exit_code,
            staged_files,
            stdout,
            stderr,
        })
    }

    fn scan_fingerprints(
        root: &Path,
        ignore_dir: &Path,
    ) -> Result<HashMap<PathBuf, (u32, Vec<u8>)>, TxError> {
        let mut map = HashMap::new();
        for entry in WalkDir::new(root)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let path = entry.path();
            if path.starts_with(ignore_dir) {
                continue;
            }
            if path.is_file()
                && let Ok(bytes) = fs::read(path) {
                    let mut hasher = Hasher::new();
                    hasher.update(&bytes);
                    map.insert(path.to_path_buf(), (hasher.finalize(), bytes));
            }
        }
        Ok(map)
    }
}
