"""
agent-2pc Python SDK: Two-Phase Commit Transaction Manager for AI Agents.
"""

import json
import subprocess
import shutil
from typing import Optional, Dict, Any, List
from contextlib import contextmanager


class TxSession:
    """
    Active transaction session bound to a specific Transaction ID (TID).

    Provides methods to stage changes in Phase 1, create checkpoints,
    inspect diffs, and finalize via commit or abort in Phase 2.
    """

    def __init__(self, client: "Agent2PC", tid: str):
        self.client = client
        self.tid = tid
        self.committed = False
        self.aborted = False

    def stage_write(
        self,
        path: str,
        content: str,
        create_dirs: bool = True,
        participant: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Phase 1: Stage file write with .bak snapshotting into isolated scratch memory.

        Args:
            path: Target file destination path.
            content: Text or binary content to write.
            create_dirs: Automatically create parent directories if missing (default: True).
            participant: Optional custom participant identifier.

        Returns:
            Dict containing participant receipt, vote status, and backup metadata.
        """
        return self.client.stage_write(
            tid=self.tid,
            path=path,
            content=content,
            create_dirs=create_dirs,
            participant=participant,
        )

    def stage_patch(
        self,
        path: str,
        patch: str,
        participant: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Phase 1: Stage a unified diff patch in scratch memory.

        Args:
            path: Destination file path to patch.
            patch: Unified diff string.
            participant: Optional custom participant identifier.

        Returns:
            Dict containing participant receipt and prepare vote status.
        """
        return self.client.stage_patch(
            tid=self.tid,
            path=path,
            patch=patch,
            participant=participant,
        )

    def stage_replace(
        self,
        path: str,
        old_string: str,
        new_string: str,
        participant: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Phase 1: Stage targeted unique substring replacement with uniqueness validation.

        Args:
            path: Target file path.
            old_string: Exact unique substring to find and replace.
            new_string: Replacement text.
            participant: Optional custom participant identifier.

        Returns:
            Dict containing participant receipt and prepare vote status.
        """
        return self.client.stage_replace(
            tid=self.tid,
            path=path,
            old_string=old_string,
            new_string=new_string,
            participant=participant,
        )

    def stage_cmd(
        self,
        command: str,
        working_dir: Optional[str] = None,
        participant: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Phase 1: Stage and verify a shell command in an isolated scratch sandbox.

        Args:
            command: Shell command string to execute and assert exit code 0.
            working_dir: Working directory for command execution.
            participant: Optional custom participant identifier.

        Returns:
            Dict containing participant receipt and prepare vote status.
        """
        return self.client.stage_cmd(
            tid=self.tid,
            command=command,
            working_dir=working_dir,
            participant=participant,
        )

    def stage_api(
        self,
        endpoint: str,
        method: str = "POST",
        idempotency_key: Optional[str] = None,
        compensation_endpoint: Optional[str] = None,
        participant: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Phase 1: Stage external REST API action with idempotency lease & compensation rollback hook.

        Args:
            endpoint: Target HTTP/HTTPS endpoint URL.
            method: HTTP method (e.g. POST, PUT, DELETE, GET). Default is POST.
            idempotency_key: Custom idempotency key (auto-generated if omitted).
            compensation_endpoint: Compensation rollback URL to call on abort.
            participant: Optional custom participant identifier.

        Returns:
            Dict containing participant receipt and lease reservation status.
        """
        return self.client.stage_api(
            tid=self.tid,
            endpoint=endpoint,
            method=method,
            idempotency_key=idempotency_key,
            compensation_endpoint=compensation_endpoint,
            participant=participant,
        )

    def record_read(self, path: str) -> str:
        """
        Register file fingerprint in read-set for optimistic concurrency conflict detection (OCC).

        Args:
            path: File path to fingerprint and track.

        Returns:
            Hex hash string representing file content fingerprint.
        """
        return self.client.record_read(tid=self.tid, path=path)

    def diff(self, stat: bool = False) -> Dict[str, Any]:
        """
        Inspect unified diff or summary diffstat of staged changes in this transaction.

        Args:
            stat: If True, return summary diffstat counts rather than full unified diff text.

        Returns:
            Dict containing file diffs, additions, deletions, and stats.
        """
        return self.client.diff(tid=self.tid, stat=stat)

    def export_bundle(self, output: Optional[str] = None) -> Dict[str, Any]:
        """
        Export active transaction and all staged artifacts to a portable verified .a2pc bundle.

        Args:
            output: Destination file path (.a2pc). Defaults to <tid>.a2pc if omitted.

        Returns:
            Dict containing bundle metadata (output_path, crc32, participant_count, file_count).
        """
        return self.client.export_bundle(tid=self.tid, output=output)

    def git_commit(self, message: str, working_dir: Optional[str] = None) -> Dict[str, Any]:
        """
        Atomically commit 2PC transaction and create a Git commit in one unified step.

        Args:
            message: Git commit message.
            working_dir: Target Git repository root directory.

        Returns:
            Dict containing commit_hash, files_committed, and commit status.
        """
        res = self.client.git_commit(tid=self.tid, message=message, working_dir=working_dir)
        self.committed = True
        return res

    def savepoint(self, name: str) -> Dict[str, Any]:
        """
        Mark a named transactional checkpoint for speculative execution and sub-task branching.

        Args:
            name: Identifier name for the savepoint checkpoint.

        Returns:
            Dict containing savepoint creation confirmation and timestamp.
        """
        return self.client.savepoint(tid=self.tid, name=name)

    def rollback_to(self, name: str) -> Dict[str, Any]:
        """
        Unwind staged steps back to a named savepoint without aborting the entire transaction.

        Args:
            name: Savepoint name to restore back to.

        Returns:
            Dict containing rollback report and reverted participant count.
        """
        return self.client.rollback_to(tid=self.tid, name=name)

    def exec(self, command: List[str], working_dir: Optional[str] = None) -> Dict[str, Any]:
        """
        Execute arbitrary command and auto-stage any created or modified files into Phase 1.

        Args:
            command: Command and argument list to execute (e.g. ["npm", "run", "build"]).
            working_dir: Directory where the command will be run.

        Returns:
            Dict containing command exit status, stdout, stderr, and auto-staged files.
        """
        return self.client.exec(tid=self.tid, command=command, working_dir=working_dir)

    def status(self) -> Dict[str, Any]:
        """
        Inspect active transaction state, receipts, and enrolled participants.

        Returns:
            Dict containing transaction state (ACTIVE, PREPARED, COMMITTED, ABORTED),
            receipts count, and participant lists.
        """
        return self.client.status(tid=self.tid)

    def commit(self) -> Dict[str, Any]:
        """
        Phase 2: Atomically commit all staged operations across all participants.

        Returns:
            Dict containing commit confirmation and final state.
        """
        if self.committed or self.aborted:
            raise RuntimeError("Transaction already finalized")
        res = self.client.commit(tid=self.tid)
        self.committed = True
        return res

    def abort(self, reason: str = "Client abort") -> Dict[str, Any]:
        """
        Phase 2: Deterministically roll back all staged changes and restore backups.

        Args:
            reason: Human or machine-readable explanation for the abort.

        Returns:
            Dict containing abort confirmation and cleanup report.
        """
        if self.committed or self.aborted:
            return {}
        res = self.client.abort(tid=self.tid, reason=reason)
        self.aborted = True
        return res


class Agent2PC:
    """
    Client for driving the agent-2pc Two-Phase Commit transaction coordinator.

    Supports direct CLI invocation and Unix Domain Socket daemon IPC.
    """

    def __init__(self, binary_path: Optional[str] = None, tx_dir: str = ".agent-tx"):
        """
        Initialize Agent2PC client.

        Args:
            binary_path: Optional custom path to agent-2pc binary.
            tx_dir: Workspace transaction directory path (default: .agent-tx).
        """
        self.bin = binary_path or shutil.which("agent-2pc") or "agent-2pc"
        self.tx_dir = tx_dir

    def _run(self, args: List[str]) -> Dict[str, Any]:
        """Execute agent-2pc command with JSON output and parse result."""
        cmd = [self.bin, "--tx-dir", self.tx_dir, "--json"] + args
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            err = proc.stderr.strip() or proc.stdout.strip()
            raise RuntimeError(f"agent-2pc error: {err}")
        try:
            return json.loads(proc.stdout)
        except Exception:
            return {"output": proc.stdout.strip()}

    def init(self) -> Dict[str, Any]:
        """
        Initialize .agent-tx workspace in current directory.

        Returns:
            Dict containing initialization status and tx_dir path.
        """
        return self._run(["init"])

    def begin(self, timeout_ms: int = 30000) -> TxSession:
        """
        Start a new transaction session and obtain a unique TID.

        Args:
            timeout_ms: Transaction timeout in milliseconds (default: 30000).

        Returns:
            TxSession bound to the allocated transaction ID.
        """
        res = self._run(["begin", "--timeout", str(timeout_ms)])
        tid = res.get("tid")
        if not tid:
            raise RuntimeError("Failed to obtain transaction ID")
        return TxSession(self, tid)

    @contextmanager
    def transaction(self, timeout_ms: int = 30000):
        """
        Context manager: automatically commits on clean block exit, rolls back on exception.

        Args:
            timeout_ms: Transaction timeout in milliseconds (default: 30000).

        Yields:
            TxSession bound to the transaction.
        """
        session = self.begin(timeout_ms=timeout_ms)
        try:
            yield session
            if not session.committed and not session.aborted:
                session.commit()
        except Exception as e:
            if not session.committed and not session.aborted:
                session.abort(reason=str(e))
            raise

    def stage_write(
        self,
        tid: str,
        path: str,
        content: str,
        create_dirs: bool = True,
        participant: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Stage an atomic file write in Phase 1 (FsAdapter).

        Args:
            tid: Target transaction ID.
            path: Destination file path.
            content: File text content.
            create_dirs: Automatically create parent directories if missing (default: True).
            participant: Optional custom participant identifier.

        Returns:
            Dict containing prepare receipt.
        """
        args = [
            "stage-write",
            "--tid", tid,
            "--path", path,
            "--content", content,
        ]
        if create_dirs:
            args.append("--create-dirs")
        if participant:
            args.extend(["--participant", participant])
        return self._run(args)

    def stage_patch(
        self,
        tid: str,
        path: str,
        patch: str,
        participant: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Stage a file patch in Phase 1 (FsAdapter).

        Args:
            tid: Target transaction ID.
            path: Destination file path.
            patch: Unified diff patch content.
            participant: Optional custom participant identifier.

        Returns:
            Dict containing prepare receipt.
        """
        args = [
            "stage-patch",
            "--tid", tid,
            "--path", path,
            f"--patch={patch}",
        ]
        if participant:
            args.extend(["--participant", participant])
        return self._run(args)

    def stage_replace(
        self,
        tid: str,
        path: str,
        old_string: str,
        new_string: str,
        participant: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Stage a targeted substring replacement in Phase 1 (FsAdapter).

        Args:
            tid: Target transaction ID.
            path: Target file path.
            old_string: Exact unique substring to replace.
            new_string: Replacement text.
            participant: Optional custom participant identifier.

        Returns:
            Dict containing prepare receipt.
        """
        args = [
            "stage-replace",
            "--tid", tid,
            "--path", path,
            "--old-string", old_string,
            "--new-string", new_string,
        ]
        if participant:
            args.extend(["--participant", participant])
        return self._run(args)

    def stage_cmd(
        self,
        tid: str,
        command: str,
        working_dir: Optional[str] = None,
        participant: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Stage and verify a shell command execution in Phase 1 (CommandAdapter).

        Args:
            tid: Target transaction ID.
            command: Shell command string to verify.
            working_dir: Optional working directory.
            participant: Optional custom participant identifier.

        Returns:
            Dict containing prepare receipt.
        """
        args = ["stage-cmd", "--tid", tid, "--command", command]
        if working_dir:
            args.extend(["--dir", working_dir])
        if participant:
            args.extend(["--participant", participant])
        return self._run(args)

    def stage_api(
        self,
        tid: str,
        endpoint: str,
        method: str = "POST",
        idempotency_key: Optional[str] = None,
        compensation_endpoint: Optional[str] = None,
        participant: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Stage an external REST API action with idempotency lease & compensation (ApiAdapter).

        Args:
            tid: Target transaction ID.
            endpoint: Remote endpoint URL.
            method: HTTP method (default: POST).
            idempotency_key: Optional custom idempotency key.
            compensation_endpoint: Optional compensation rollback URL.
            participant: Optional custom participant identifier.

        Returns:
            Dict containing prepare receipt and lease.
        """
        args = ["stage-api", "--tid", tid, "--endpoint", endpoint, "--method", method]
        if idempotency_key:
            args.extend(["--idempotency-key", idempotency_key])
        if compensation_endpoint:
            args.extend(["--compensation-endpoint", compensation_endpoint])
        if participant:
            args.extend(["--participant", participant])
        return self._run(args)

    def record_read(self, tid: str, path: str) -> str:
        """
        Record a file in the transaction's read-set for optimistic concurrency conflict detection.

        Args:
            tid: Target transaction ID.
            path: Target file path to fingerprint.

        Returns:
            Hex hash string representing file fingerprint.
        """
        res = self._run(["record-read", "--tid", tid, "--path", path])
        return res.get("hash", "")

    def commit(self, tid: str) -> Dict[str, Any]:
        """
        Phase 2: Commit all staged changes atomically.

        Args:
            tid: Target transaction ID to commit.

        Returns:
            Dict containing commit status.
        """
        return self._run(["commit", "--tid", tid])

    def abort(self, tid: str, reason: str = "Client abort") -> Dict[str, Any]:
        """
        Phase 2: Abort transaction and deterministically roll back staged changes.

        Args:
            tid: Target transaction ID to abort.
            reason: Reason for abort.

        Returns:
            Dict containing abort status.
        """
        return self._run(["abort", "--tid", tid, "--reason", reason])

    def status(self, tid: Optional[str] = None) -> Dict[str, Any]:
        """
        Inspect active transaction states and participants.

        Args:
            tid: Optional specific transaction ID to query.

        Returns:
            Dict containing status and metadata.
        """
        args = ["status"]
        if tid:
            args.extend(["--tid", tid])
        return self._run(args)

    def savepoint(self, tid: str, name: str) -> Dict[str, Any]:
        """
        Create a transactional savepoint checkpoint within an active transaction.

        Args:
            tid: Target transaction ID.
            name: Savepoint identifier name.

        Returns:
            Dict containing savepoint confirmation.
        """
        return self._run(["savepoint", "--tid", tid, "--name", name])

    def rollback_to(self, tid: str, name: str) -> Dict[str, Any]:
        """
        Roll back transaction changes staged after the specified savepoint.

        Args:
            tid: Target transaction ID.
            name: Savepoint name to restore back to.

        Returns:
            Dict containing rollback report.
        """
        return self._run(["rollback-to", "--tid", tid, "--name", name])

    def exec(self, tid: str, command: List[str], working_dir: Optional[str] = None) -> Dict[str, Any]:
        """
        Execute arbitrary command and auto-stage any created or modified files into Phase 1.

        Args:
            tid: Target transaction ID.
            command: Command and arguments list.
            working_dir: Optional working directory.

        Returns:
            Dict containing execution report and staged files.
        """
        args = ["exec", "--tid", tid]
        if working_dir:
            args.extend(["--dir", working_dir])
        args.append("--")
        args.extend(command)
        return self._run(args)

    def diff(self, tid: str, stat: bool = False) -> Dict[str, Any]:
        """
        Display unified diff of all staged file modifications in an active transaction.

        Args:
            tid: Target transaction ID.
            stat: Display summary diffstat only if True.

        Returns:
            Dict containing diff or diffstat report.
        """
        args = ["diff", "--tid", tid]
        if stat:
            args.append("--stat")
        return self._run(args)

    def export_bundle(self, tid: str, output: Optional[str] = None) -> Dict[str, Any]:
        """
        Export an active transaction to a portable .a2pc bundle.

        Args:
            tid: Target transaction ID.
            output: Destination file path (.a2pc).

        Returns:
            Dict containing bundle summary and CRC32 checksum.
        """
        args = ["export", "--tid", tid]
        if output:
            args.extend(["--output", output])
        return self._run(args)

    def import_bundle(self, file: str) -> Dict[str, Any]:
        """
        Import a transaction bundle (.a2pc) and register as a prepared transaction.

        Args:
            file: Path to .a2pc bundle file.

        Returns:
            Dict containing imported transaction ID, restored files, and PREPARED status.
        """
        return self._run(["import", "--file", file])

    def git_commit(self, tid: str, message: str, working_dir: Optional[str] = None) -> Dict[str, Any]:
        """
        Atomically commit 2PC transaction and create a Git commit in one step.

        Args:
            tid: Target transaction ID.
            message: Git commit message.
            working_dir: Target Git repository directory.

        Returns:
            Dict containing commit hash and committed files.
        """
        args = ["git-commit", "--tid", tid, f"--message={message}"]
        if working_dir:
            args.extend(["--dir", working_dir])
        return self._run(args)

    def hook_check(self) -> Dict[str, Any]:
        """
        Check whether .agent-tx is clean of pending uncommitted transactions.

        Returns:
            Dict containing clean boolean status and in-flight transaction IDs.
        """
        return self._run(["hook", "check"])

    def hook_install(self, repo_dir: str = ".") -> Dict[str, Any]:
        """
        Install pre-commit repository integrity hook into .git/hooks.

        Args:
            repo_dir: Target Git repository root.

        Returns:
            Dict containing hook installation report.
        """
        return self._run(["hook", "install", "--repo-dir", repo_dir])

    def daemon_reap(self) -> Dict[str, Any]:
        """
        Manually trigger a TTL sweep of expired transactions via the daemon.

        Returns:
            Dict containing reaped count and total reaped statistics.
        """
        return self._run(["daemon", "reap"])

    def daemon_start(self, foreground: bool = False) -> Dict[str, Any]:
        """
        Start the agent-2pc daemon.

        Args:
            foreground: Run daemon in foreground instead of spawning background worker.

        Returns:
            Dict containing daemon status and PID.
        """
        args = ["daemon", "start"]
        if foreground:
            args.append("--foreground")
        return self._run(args)

    def daemon_stop(self) -> Dict[str, Any]:
        """
        Stop the running agent-2pc daemon.

        Returns:
            Dict containing stop status.
        """
        return self._run(["daemon", "stop"])

    def daemon_status(self) -> Dict[str, Any]:
        """
        Query status of the agent-2pc daemon.

        Returns:
            Dict containing alive status and socket path.
        """
        return self._run(["daemon", "status"])

    def recover(self) -> Dict[str, Any]:
        """
        Run crash recovery scan on WAL to resolve orphaned transactions.

        Returns:
            Dict containing recovery report.
        """
        return self._run(["recover"])

    def compact(self) -> Dict[str, Any]:
        """
        Compact WAL by purging finalized transactions while preserving active in-flight state.

        Returns:
            Dict containing compaction report.
        """
        return self._run(["compact"])
