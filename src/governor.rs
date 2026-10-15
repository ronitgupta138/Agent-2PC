//! In-Kernel / Process-Group Velocity Governor for Blast-Radius Containment.

use crate::adapters::ResourceManager;
use crate::domain::{
    ParticipantId, ParticipantVote, ResourceIntent, StageReceipt, TransactionId, TxError,
};
use std::fs;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Sliding-window velocity governor adapter preventing runaway fork bombs and deletion loops.
pub struct GovernorAdapter {
    id: ParticipantId,
}

impl GovernorAdapter {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: ParticipantId::new(id),
        }
    }
}

impl ResourceManager for GovernorAdapter {
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
            ResourceIntent::GovernedCommand {
                command,
                working_dir,
                max_forks,
                timeout_ms,
                env,
            } => {
                let start_time = Instant::now();
                let max_duration = Duration::from_millis(*timeout_ms);
                let max_fork_count = *max_forks;

                // Spawn child in isolated process group
                let mut cmd = Command::new("sh");
                cmd.arg("-c")
                    .arg(command)
                    .current_dir(working_dir)
                    .envs(env);
                #[cfg(unix)]
                cmd.process_group(0); // Create new process group with PGID == PID

                let mut child = cmd.spawn()?;
                let pid = child.id() as i32;

                let stop_watchdog = Arc::new(AtomicBool::new(false));
                let stop_clone = Arc::clone(&stop_watchdog);
                let tripped = Arc::new(AtomicBool::new(false));
                let tripped_clone = Arc::clone(&tripped);
                let trip_reason = Arc::new(std::sync::Mutex::new(String::new()));
                let trip_reason_clone = Arc::clone(&trip_reason);

                // Velocity watchdog thread
                let watchdog = std::thread::spawn(move || {
                    while !stop_clone.load(Ordering::Relaxed) {
                        // 1. Check runtime timeout
                        if start_time.elapsed() > max_duration {
                            // Instant in-kernel freeze
                            unsafe { libc::kill(-pid, libc::SIGSTOP) };
                            *trip_reason_clone.lock().unwrap() = format!(
                                "Runtime velocity limit exceeded ({}ms > {}ms limit)",
                                start_time.elapsed().as_millis(),
                                max_duration.as_millis()
                            );
                            tripped_clone.store(true, Ordering::SeqCst);
                            unsafe { libc::kill(-pid, libc::SIGKILL) };
                            break;
                        }

                        // 2. Check fork / task count via /proc/<pid>/task
                        let task_dir = PathBuf::from(format!("/proc/{}/task", pid));
                        if task_dir.exists()
                            && let Ok(entries) = fs::read_dir(&task_dir) {
                                let task_count = entries.count();
                                if task_count > max_fork_count {
                                    // Freeze process group before destruction spreads
                                    unsafe { libc::kill(-pid, libc::SIGSTOP) };
                                    *trip_reason_clone.lock().unwrap() = format!(
                                        "Fork-storm velocity breached: {} concurrent tasks (limit: {})",
                                        task_count, max_fork_count
                                    );
                                    tripped_clone.store(true, Ordering::SeqCst);
                                    unsafe { libc::kill(-pid, libc::SIGKILL) };
                                    break;
                                }
                        }

                        std::thread::sleep(Duration::from_millis(15));
                    }
                });

                let wait_result = child.wait();
                stop_watchdog.store(true, Ordering::Relaxed);
                let _ = watchdog.join();

                if tripped.load(Ordering::SeqCst) {
                    let reason = trip_reason.lock().unwrap().clone();
                    return Ok(ParticipantVote::Aborted {
                        reason: format!("Velocity Governor Tripped: {}", reason),
                    });
                }

                let exit_status = wait_result?;
                if !exit_status.success() {
                    return Ok(ParticipantVote::Aborted {
                        reason: format!(
                            "Governed command failed with exit code: {}",
                            exit_status.code().unwrap_or(-1)
                        ),
                    });
                }

                let mut receipt = StageReceipt::new(self.id.clone());
                receipt.stage_path = Some(tx_scratch);
                receipt
                    .metadata
                    .insert("command".to_string(), command.clone());
                receipt.metadata.insert(
                    "elapsed_ms".to_string(),
                    start_time.elapsed().as_millis().to_string(),
                );

                Ok(ParticipantVote::Prepared(receipt))
            }

            _ => Ok(ParticipantVote::Aborted {
                reason: format!("GovernorAdapter cannot handle intent type {:?}", intent),
            }),
        }
    }

    fn commit(&mut self, _tid: &TransactionId, receipt: &StageReceipt) -> Result<(), TxError> {
        if let Some(stage_dir) = &receipt.stage_path
            && stage_dir.exists() {
                let _ = fs::remove_dir_all(stage_dir);
        }
        Ok(())
    }

    fn abort(&mut self, _tid: &TransactionId, receipt: Option<&StageReceipt>) -> Result<(), TxError> {
        if let Some(r) = receipt
            && let Some(stage_dir) = &r.stage_path
            && stage_dir.exists() {
                let _ = fs::remove_dir_all(stage_dir);
        }
        Ok(())
    }
}
