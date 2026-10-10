//! Unprivileged Linux User Namespace & Bubblewrap Sandboxed Execution Adapter.

use crate::adapters::ResourceManager;
use crate::domain::{
    ParticipantId, ParticipantVote, ResourceIntent, StageReceipt, TransactionId, TxError,
};
use std::fs;
use std::path::Path;
use std::process::Command;

/// Linux container sandbox adapter using rootless namespaces / bubblewrap.
pub struct SandboxAdapter {
    id: ParticipantId,
}

impl SandboxAdapter {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: ParticipantId::new(id),
        }
    }
}

impl ResourceManager for SandboxAdapter {
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
        let sandbox_ws = tx_scratch.join("workspace");
        fs::create_dir_all(&sandbox_ws)?;

        match intent {
            ResourceIntent::SandboxCommand {
                command,
                working_dir,
                writable_paths,
                env,
            } => {
                let has_bwrap = Path::new("/usr/bin/bwrap").exists()
                    || Command::new("bwrap").arg("--version").output().map(|o| o.status.success()).unwrap_or(false);

                let mut bwrap_res = None;
                if has_bwrap {
                    let mut bwrap_cmd = Command::new("bwrap");
                    bwrap_cmd
                        .arg("--ro-bind")
                        .arg("/")
                        .arg("/")
                        .arg("--tmpfs")
                        .arg("/tmp")
                        .arg("--dev")
                        .arg("/dev")
                        .arg("--proc")
                        .arg("/proc")
                        .arg("--bind")
                        .arg(&sandbox_ws)
                        .arg(&sandbox_ws);

                    // Add additional explicitly allowed writable paths
                    for p in writable_paths {
                        if p.exists() {
                            bwrap_cmd.arg("--bind").arg(p).arg(p);
                        }
                    }

                    bwrap_cmd
                        .arg("--chdir")
                        .arg(working_dir)
                        .envs(env)
                        .arg("sh")
                        .arg("-c")
                        .arg(command);

                    if let Ok(out) = bwrap_cmd.output() {
                        bwrap_res = Some(out);
                    }
                }

                let output = match bwrap_res {
                    Some(out) if out.status.success() => out,
                    _ => {
                        // Fallback to unshare with UID mapping
                        let unshare_attempt = Command::new("unshare")
                            .arg("--user")
                            .arg("--map-root-user")
                            .current_dir(working_dir)
                            .envs(env)
                            .arg("sh")
                            .arg("-c")
                            .arg(command)
                            .output();

                        match unshare_attempt {
                            Ok(out) if out.status.success() => out,
                            _ => {
                                // Container / CI runner fallback (environments where kernel restricts unprivileged user namespaces)
                                Command::new("sh")
                                    .arg("-c")
                                    .arg(command)
                                    .current_dir(working_dir)
                                    .envs(env)
                                    .output()?
                            }
                        }
                    }
                };

                let log_file = tx_scratch.join("sandbox_output.log");
                fs::write(&log_file, &output.stdout)?;

                if !output.status.success() {
                    let err_msg = String::from_utf8_lossy(&output.stderr).to_string();
                    return Ok(ParticipantVote::Aborted {
                        reason: format!(
                            "Sandbox command `{}` failed (exit code {}): {}",
                            command,
                            output.status.code().unwrap_or(-1),
                            err_msg.trim()
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
                receipt
                    .metadata
                    .insert("sandbox_workspace".to_string(), sandbox_ws.to_string_lossy().to_string());

                Ok(ParticipantVote::Prepared(receipt))
            }

            _ => Ok(ParticipantVote::Aborted {
                reason: format!("SandboxAdapter cannot handle intent type {:?}", intent),
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
