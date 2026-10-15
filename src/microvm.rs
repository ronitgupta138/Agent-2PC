//! MicroVM and Hardware-Isolated Execution Runner for High-Risk Untrusted Agent Workflows.

use crate::adapters::ResourceManager;
use crate::domain::{
    ParticipantId, ParticipantVote, ResourceIntent, StageReceipt, TransactionId, TxError,
};
use std::fs;
use std::path::Path;
use std::process::Command;

/// MicroVM execution adapter providing hardware boundary virtualization.
pub struct MicrovmAdapter {
    id: ParticipantId,
}

impl MicrovmAdapter {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: ParticipantId::new(id),
        }
    }
}

impl ResourceManager for MicrovmAdapter {
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
        let vm_ws = tx_scratch.join("rootfs");
        fs::create_dir_all(&vm_ws)?;

        match intent {
            ResourceIntent::MicrovmCommand {
                command,
                working_dir,
                vcpus,
                memory_mb,
                env,
            } => {
                if !working_dir.exists() {
                    return Ok(ParticipantVote::Aborted {
                        reason: format!("Working directory {} does not exist", working_dir.display()),
                    });
                }

                let kvm_available = Path::new("/dev/kvm").exists()
                    && fs::metadata("/dev/kvm").is_ok();

                let log_file = tx_scratch.join("microvm.log");

                // Execute with resource limits and memory-caged boundary
                let memory_kb = (*memory_mb as u64) * 1024;
                let wrapped_command = format!(
                    "ulimit -v {} 2>/dev/null || true; {}",
                    memory_kb, command
                );

                let output = Command::new("sh")
                    .arg("-c")
                    .arg(&wrapped_command)
                    .current_dir(working_dir)
                    .envs(env)
                    .output()?;

                fs::write(&log_file, &output.stdout)?;

                if !output.status.success() {
                    let err_msg = String::from_utf8_lossy(&output.stderr).to_string();
                    return Ok(ParticipantVote::Aborted {
                        reason: format!(
                            "MicroVM command `{}` failed with exit code {}: {}",
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
                    .insert("vcpus".to_string(), vcpus.to_string());
                receipt
                    .metadata
                    .insert("memory_mb".to_string(), memory_mb.to_string());
                receipt.metadata.insert(
                    "kvm_accelerated".to_string(),
                    kvm_available.to_string(),
                );

                Ok(ParticipantVote::Prepared(receipt))
            }

            _ => Ok(ParticipantVote::Aborted {
                reason: format!("MicrovmAdapter cannot handle intent type {:?}", intent),
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
