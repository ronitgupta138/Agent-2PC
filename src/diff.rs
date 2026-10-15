//! Unified Diff Generator for Staged 2PC Transactions.
//!
//! Inspects staged file mutations within an active 2PC transaction,
//! generating git-style unified diffs and summary stats without touching working code.

use crate::coordinator::Coordinator;
use crate::domain::{TransactionId, TxError};
use colored::Colorize;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

/// Single file modification entry within a transaction diff.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileDiff {
    pub path: PathBuf,
    pub is_new: bool,
    pub is_deleted: bool,
    pub additions: usize,
    pub deletions: usize,
    pub unified_diff: String,
}

/// Full unified diff representation of all staged mutations in a transaction.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransactionDiff {
    pub tid: TransactionId,
    pub files: Vec<FileDiff>,
    pub total_additions: usize,
    pub total_deletions: usize,
}

impl TransactionDiff {
    /// Render diff for terminal display with optional ANSI coloring.
    pub fn render_terminal(&self, color: bool) -> String {
        if self.files.is_empty() {
            return "No staged filesystem modifications in transaction.\n".to_string();
        }

        let mut out = String::new();
        for f in &self.files {
            if color {
                out.push_str(&format!("{} a/{} b/{}\n", "diff --git".bold(), f.path.display(), f.path.display()));
                out.push_str(&format!("--- a/{}\n", f.path.display()).red().to_string());
                out.push_str(&format!("+++ b/{}\n", f.path.display()).green().to_string());
                for line in f.unified_diff.lines() {
                    if line.starts_with('+') {
                        out.push_str(&line.green().to_string());
                    } else if line.starts_with('-') {
                        out.push_str(&line.red().to_string());
                    } else if line.starts_with('@') {
                        out.push_str(&line.cyan().to_string());
                    } else {
                        out.push_str(line);
                    }
                    out.push('\n');
                }
            } else {
                out.push_str(&format!("diff --git a/{0} b/{0}\n", f.path.display()));
                out.push_str(&format!("--- a/{}\n", f.path.display()));
                out.push_str(&format!("+++ b/{}\n", f.path.display()));
                out.push_str(&f.unified_diff);
                if !f.unified_diff.ends_with('\n') {
                    out.push('\n');
                }
            }
        }
        out
    }

    /// Render summary stats (files changed, additions, deletions).
    pub fn render_stat(&self) -> String {
        if self.files.is_empty() {
            return " 0 files changed, 0 insertions(+), 0 deletions(-)\n".to_string();
        }

        let mut out = String::new();
        for f in &self.files {
            out.push_str(&format!(
                " {:<40} | {:>3} +{:<3} -{:<3}\n",
                f.path.display(),
                f.additions + f.deletions,
                f.additions,
                f.deletions
            ));
        }
        out.push_str(&format!(
            " {} files changed, {} insertions(+), {} deletions(-)\n",
            self.files.len(),
            self.total_additions,
            self.total_deletions
        ));
        out
    }
}

pub struct TransactionDiffer;

impl TransactionDiffer {
    /// Compute unified diff for all staged files in the specified transaction.
    pub fn diff_transaction(
        coord: &Coordinator,
        tid: &TransactionId,
    ) -> Result<TransactionDiff, TxError> {
        let tx = coord
            .get_status(tid)
            .ok_or(TxError::TransactionNotFound(*tid))?;

        let mut files = Vec::new();
        let mut total_additions = 0;
        let mut total_deletions = 0;

        for receipt in tx.receipts.values() {
            let target_str = match receipt.metadata.get("target_path") {
                Some(s) => s,
                None => continue,
            };
            let target_path = PathBuf::from(target_str);

            let stage_dir = match &receipt.stage_path {
                Some(p) => p,
                None => continue,
            };

            let staged_payload = stage_dir.join("staged_payload");
            let staged_patch = stage_dir.join("staged_patch");

            // Read original content from backup or target path
            let original_content = if let Some(backup) = receipt.backup_paths.first() {
                if backup.exists() {
                    fs::read_to_string(backup).unwrap_or_default()
                } else if target_path.exists() {
                    fs::read_to_string(&target_path).unwrap_or_default()
                } else {
                    String::new()
                }
            } else if target_path.exists() {
                fs::read_to_string(&target_path).unwrap_or_default()
            } else {
                String::new()
            };

            let staged_content = if staged_payload.exists() {
                fs::read_to_string(&staged_payload).unwrap_or_default()
            } else if staged_patch.exists() {
                let patch = fs::read_to_string(&staged_patch).unwrap_or_default();
                if original_content.is_empty() {
                    patch
                } else {
                    let mut s = original_content.clone();
                    if !s.ends_with('\n') {
                        s.push('\n');
                    }
                    s.push_str(&patch);
                    s
                }
            } else {
                continue;
            };

            let is_new = original_content.is_empty() && !target_path.exists();
            let (diff_text, adds, dels) = compute_unified_diff(&original_content, &staged_content);

            total_additions += adds;
            total_deletions += dels;

            files.push(FileDiff {
                path: target_path,
                is_new,
                is_deleted: false,
                additions: adds,
                deletions: dels,
                unified_diff: diff_text,
            });
        }

        Ok(TransactionDiff {
            tid: *tid,
            files,
            total_additions,
            total_deletions,
        })
    }
}

/// Compute simple unified diff hunks between original and modified text.
fn compute_unified_diff(orig: &str, modified: &str) -> (String, usize, usize) {
    let orig_lines: Vec<&str> = if orig.is_empty() {
        Vec::new()
    } else {
        orig.lines().collect()
    };
    let mod_lines: Vec<&str> = if modified.is_empty() {
        Vec::new()
    } else {
        modified.lines().collect()
    };

    let mut out = String::new();
    let mut additions = 0;
    let mut deletions = 0;

    if orig_lines.is_empty() {
        // Pure additions
        out.push_str(&format!("@@ -0,0 +1,{} @@\n", mod_lines.len()));
        for line in &mod_lines {
            out.push_str(&format!("+{}\n", line));
            additions += 1;
        }
        return (out, additions, deletions);
    }

    if mod_lines.is_empty() {
        // Pure deletions
        out.push_str(&format!("@@ -1,{} +0,0 @@\n", orig_lines.len()));
        for line in &orig_lines {
            out.push_str(&format!("-{}\n", line));
            deletions += 1;
        }
        return (out, additions, deletions);
    }

    out.push_str(&format!("@@ -1,{} +1,{} @@\n", orig_lines.len(), mod_lines.len()));

    let mut o = 0;
    let mut m = 0;

    while o < orig_lines.len() || m < mod_lines.len() {
        if o < orig_lines.len() && m < mod_lines.len() && orig_lines[o] == mod_lines[m] {
            out.push_str(&format!(" {}\n", orig_lines[o]));
            o += 1;
            m += 1;
        } else if o < orig_lines.len() && (m >= mod_lines.len() || !mod_lines.contains(&orig_lines[o])) {
            out.push_str(&format!("-{}\n", orig_lines[o]));
            deletions += 1;
            o += 1;
        } else if m < mod_lines.len() {
            out.push_str(&format!("+{}\n", mod_lines[m]));
            additions += 1;
            m += 1;
        } else {
            o += 1;
            m += 1;
        }
    }

    (out, additions, deletions)
}
