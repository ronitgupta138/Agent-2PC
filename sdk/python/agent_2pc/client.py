"""
agent-2pc Python SDK: Two-Phase Commit Transaction Manager for AI Agents.
"""

import json
import subprocess
import shutil
from typing import Optional, Dict, Any, List
from contextlib import contextmanager

class TxSession:
    def __init__(self, client: "Agent2PC", tid: str):
        self.client = client
        self.tid = tid
        self.committed = False
        self.aborted = False

    def stage_write(self, path: str, content: str, create_dirs: bool = True) -> Dict[str, Any]:
        """Phase 1: Stage file write with .bak snapshotting."""
        return self.client._run([
            "stage-write",
            "--tid", self.tid,
            "--path", path,
            "--content", content,
            "--create-dirs", str(create_dirs).lower()
        ])

    def stage_cmd(self, command: str) -> Dict[str, Any]:
        """Phase 1: Stage and verify command in sandbox."""
        return self.client._run([
            "stage-cmd",
            "--tid", self.tid,
            "--command", command
        ])

    def stage_replace(self, path: str, old_string: str, new_string: str) -> Dict[str, Any]:
        """Phase 1: Stage targeted unique substring replacement."""
        return self.client._run([
            "stage-replace",
            "--tid", self.tid,
            "--path", path,
            "--old-string", old_string,
            "--new-string", new_string
        ])

    def diff(self, stat: bool = False) -> Dict[str, Any]:
        """Inspect unified diff or diffstat of staged changes."""
        args = ["diff", "--tid", self.tid]
        if stat:
            args.append("--stat")
        return self.client._run(args)

    def export_bundle(self, output: Optional[str] = None) -> Dict[str, Any]:
        """Export transaction to portable .a2pc bundle."""
        args = ["export", "--tid", self.tid]
        if output:
            args.extend(["--output", output])
        return self.client._run(args)

    def record_read(self, path: str) -> str:
        """Register file fingerprint in read-set for OCC conflict detection."""
        res = self.client._run([
            "record-read",
            "--tid", self.tid,
            "--path", path
        ])
        return res.get("hash", "")

    def savepoint(self, name: str) -> Dict[str, Any]:
        """Mark a nested transactional checkpoint for speculative execution."""
        return self.client._run([
            "savepoint",
            "--tid", self.tid,
            "--name", name
        ])

    def rollback_to(self, name: str) -> Dict[str, Any]:
        """Unwind staged steps back to a named savepoint without aborting entire transaction."""
        return self.client._run([
            "rollback-to",
            "--tid", self.tid,
            "--name", name
        ])

    def exec(self, command: List[str], working_dir: Optional[str] = None) -> Dict[str, Any]:
        """Execute command and auto-stage any created or modified files into Phase 1."""
        args = ["exec", "--tid", self.tid]
        if working_dir:
            args.extend(["--dir", working_dir])
        args.append("--")
        args.extend(command)
        return self.client._run(args)

    def commit(self) -> Dict[str, Any]:
        """Phase 2: Atomically commit all staged operations."""
        if self.committed or self.aborted:
            raise RuntimeError("Transaction already finalized")
        res = self.client._run(["commit", "--tid", self.tid])
        self.committed = True
        return res

    def abort(self, reason: str = "Client abort") -> Dict[str, Any]:
        """Phase 2: Deterministically roll back all staged changes."""
        if self.committed or self.aborted:
            return {}
        res = self.client._run(["abort", "--tid", self.tid, "--reason", reason])
        self.aborted = True
        return res


class Agent2PC:
    """Client for driving agent-2pc transaction coordinator."""

    def __init__(self, binary_path: Optional[str] = None, tx_dir: str = ".agent-tx"):
        self.bin = binary_path or shutil.which("agent-2pc") or "agent-2pc"
        self.tx_dir = tx_dir

    def _run(self, args: List[str]) -> Dict[str, Any]:
        cmd = [self.bin, "--tx-dir", self.tx_dir, "--json"] + args
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            err = proc.stderr.strip() or proc.stdout.strip()
            raise RuntimeError(f"agent-2pc error: {err}")
        try:
            return json.loads(proc.stdout)
        except Exception:
            return {"output": proc.stdout.strip()}

    def begin(self, timeout_ms: int = 30000) -> TxSession:
        """Start a new transaction session."""
        res = self._run(["begin", "--timeout", str(timeout_ms)])
        tid = res.get("tid")
        if not tid:
            raise RuntimeError("Failed to obtain transaction ID")
        return TxSession(self, tid)

    @contextmanager
    def transaction(self, timeout_ms: int = 30000):
        """Context manager: commits on clean exit, rolls back on exception."""
        session = self.begin(timeout_ms=timeout_ms)
        try:
            yield session
            if not session.committed and not session.aborted:
                session.commit()
        except Exception as e:
            if not session.committed and not session.aborted:
                session.abort(reason=str(e))
            raise
