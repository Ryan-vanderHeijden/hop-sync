"""Transfer jobs: a queue that runs a few copies at once and survives drops."""

import asyncio
import dataclasses
import enum
import stat
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field

import asyncssh

from .copy import copy_path
from .errors import TransferError
from .events import (
    EntryFailed,
    EntrySkipped,
    TransferFinished,
    TransferProgress,
    TransferStarted,
    TreeProgress,
)
from .paths import RemotePath
from .session import ConnectError, HostKeyError, KeyNeedsPassphrase
from .sessions import SessionPool
from .transfer import resolve_destination

PROGRESS_INTERVAL = 0.2  # seconds between progress events per job
DEFAULT_BACKOFF = (2, 4, 8, 16, 32)  # seconds before reconnect attempts 1..5


class JobState(enum.Enum):
    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class JobOptions:
    verify: bool = False
    links: bool = False
    overwrite: bool = False


@dataclass
class Job:
    id: str
    src: RemotePath
    dst: RemotePath
    options: JobOptions
    state: JobState = JobState.QUEUED
    bytes_done: int = 0
    bytes_total: int = 0
    bytes_transferred: int = 0  # bytes actually sent (not skipped files, not resumed prefixes)
    files_done: int = 0
    files_found: int = 0
    walk_complete: bool = False
    current_file: str = ""
    message: str = ""  # an error, or "reconnecting (attempt 2/5) in 4s"
    error_kind: str | None = None  # "auth", "passphrase", "hostkey", "connection" or "other"
    error_alias: str | None = None  # the host an auth/key problem is about
    attempt: int = 1  # incremented by every manual retry
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    finished_at: float | None = None


@dataclass(frozen=True)
class JobChanged:
    """A snapshot (an independent copy) of a job after a change."""

    job: Job


@dataclass(frozen=True)
class JobEntryEvent:
    """An EntrySkipped/EntryFailed event from inside a job."""

    job_id: str
    event: object


def is_connection_error(error: BaseException) -> bool:
    """True for errors that mean 'the link broke', which are worth retrying."""
    if isinstance(error, asyncssh.PermissionDenied):
        return False  # a login problem, never retried
    return isinstance(
        error,
        (
            asyncssh.SFTPConnectionLost,
            asyncssh.SFTPNoConnection,
            asyncssh.DisconnectError,  # includes ConnectionLost
            ConnectionError,
            TimeoutError,
            OSError,
        ),
    )


class _Progress:
    """Turns copy events into job counters for one attempt."""

    def __init__(self, manager: "JobManager", job: Job) -> None:
        self.manager, self.job = manager, job
        self.tree_done = 0  # bytes of finished files (trees only)
        self.current = 0  # bytes of the file being copied now
        self.last_file = 0  # position of the previous progress event in this file
        self.is_tree = False

    def __call__(self, event) -> None:
        job = self.job
        if isinstance(event, TransferStarted):
            self.current = self.last_file = event.resumed_from
            job.current_file = event.src
            if not self.is_tree:
                job.bytes_total, job.files_found = event.total_bytes, 1
        elif isinstance(event, TransferProgress):
            job.bytes_transferred += event.bytes_done - self.last_file
            self.current = self.last_file = event.bytes_done
        elif isinstance(event, TransferFinished):
            self.current = 0
            if not self.is_tree:
                self.tree_done = event.bytes_done
        elif isinstance(event, TreeProgress):
            self.is_tree = True
            self.tree_done = event.bytes_done
            job.files_done, job.files_found = event.files_done, event.files_found
            job.bytes_total, job.walk_complete = event.bytes_found, event.walk_complete
        elif isinstance(event, (EntrySkipped, EntryFailed)):
            self.manager._on_event(JobEntryEvent(job.id, event))
            return
        else:
            return
        job.bytes_done = self.tree_done + self.current
        self.manager._emit(job, throttle=True)


class JobManager:
    """Runs jobs, at most `max_concurrent` at a time."""

    def __init__(
        self,
        pool: SessionPool,
        on_event: Callable,
        max_concurrent: int = 2,
        *,
        backoff=DEFAULT_BACKOFF,
        chunk_size: int = 4 * 1024 * 1024,
        copier=copy_path,
        resolver=resolve_destination,
    ) -> None:
        self._pool = pool
        self._on_event = on_event
        self._slots = asyncio.Semaphore(max_concurrent)
        self._backoff = tuple(backoff)
        self._chunk_size = chunk_size
        self._copier = copier
        self._resolver = resolver
        self._jobs: dict[str, Job] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        # Kept for a job's whole life (also across retries):
        self._created_dirs: dict[str, set] = {}
        self._resolved: dict[str, str] = {}
        self._last_emit: dict[str, float] = {}

    # ---- public API ----------------------------------------------------

    def add(self, src: RemotePath, dst: RemotePath, **options) -> Job:
        """Queue a copy. Same-host copies are refused (relaying is pointless)."""
        if src.host == dst.host:
            raise TransferError("source and destination are on the same host; copy on the host itself")
        job = Job(uuid.uuid4().hex[:8], src, dst, JobOptions(**options))
        self._jobs[job.id] = job
        self._created_dirs[job.id] = set()
        self._emit(job)
        self._start(job)
        return job

    def get(self, job_id: str) -> Job:
        return self._jobs[job_id]

    def list(self) -> list[Job]:
        return list(self._jobs.values())

    def cancel(self, job_id: str) -> None:
        """Cancel a queued or running job. Partial files are kept."""
        job = self._jobs[job_id]
        if job.state is JobState.QUEUED:
            self._finish(job, JobState.CANCELLED)  # set now: its task may not have started
            self._tasks[job_id].cancel()
        elif job.state is JobState.RUNNING:
            self._tasks[job_id].cancel()  # the task records the final state

    def cancel_all(self) -> None:
        for job in self.list():
            self.cancel(job.id)

    def retry(self, job_id: str) -> Job:
        """Run a failed or cancelled job again; it resumes where it stopped."""
        job = self._jobs[job_id]
        if job.state not in (JobState.FAILED, JobState.CANCELLED):
            raise TransferError(f"job {job_id} is {job.state.value}; only failed or cancelled jobs can be retried")
        if not self._tasks[job_id].done():
            raise TransferError(f"job {job_id} is still stopping")
        job.state = JobState.QUEUED
        job.bytes_done = job.bytes_total = job.bytes_transferred = job.files_done = job.files_found = 0
        job.walk_complete = False
        job.current_file = job.message = ""
        job.error_kind = job.error_alias = None
        job.started_at = job.finished_at = None
        job.attempt += 1
        self._emit(job)
        self._start(job)
        return job

    async def wait_idle(self) -> None:
        """Wait until no job is queued or running."""
        while True:
            pending = [t for t in self._tasks.values() if not t.done()]
            if not pending:
                return
            await asyncio.gather(*pending, return_exceptions=True)

    async def shutdown(self) -> None:
        self.cancel_all()
        await self.wait_idle()

    # ---- running a job -------------------------------------------------

    def _start(self, job: Job) -> None:
        self._tasks[job.id] = asyncio.create_task(self._run(job))

    async def _run(self, job: Job) -> None:
        try:
            async with self._slots:  # waiting here = queued
                job.state = JobState.RUNNING
                job.started_at = time.time()
                self._emit(job)
                await self._run_with_retries(job)
        except asyncio.CancelledError:
            if job.state is not JobState.CANCELLED:
                self._finish(job, JobState.CANCELLED)
            # Not re-raised: we asked for this cancel and recorded the state.
        except Exception as error:  # noqa: BLE001 - every failure becomes a job state
            self._fail(job, error)

    async def _run_with_retries(self, job: Job) -> None:
        """Run the copy; on a dropped connection wait and try again."""
        for retry_number in range(len(self._backoff) + 1):
            used = []  # endpoints this attempt used, so we invalidate the right ones
            try:
                await self._attempt(job, used)
                return
            except Exception as error:
                dropped = is_connection_error(error) or any(e.conn.is_closed() for e in used)
                if isinstance(error, ConnectError) or not dropped:
                    raise  # auth/host-key/other errors are never retried
                if retry_number == len(self._backoff):
                    raise _GaveUp(f"connection lost: {error or type(error).__name__}") from error
                for endpoint in used:
                    self._pool.invalidate(endpoint.alias, only_if=endpoint)
                delay = self._backoff[retry_number]
                job.message = f"reconnecting (attempt {retry_number + 1}/{len(self._backoff)}) in {delay}s"
                self._emit(job)
                await asyncio.sleep(delay)

    async def _attempt(self, job: Job, used: list) -> None:
        src_end = await self._pool.get(job.src.host)
        used.append(src_end)
        dst_end = await self._pool.get(job.dst.host)
        used.append(dst_end)
        job.message = ""
        # Resolve the destination once: after a first attempt created a new
        # top directory, resolving again would wrongly copy INTO it.
        if job.id not in self._resolved:
            src_attrs = await src_end.sftp.stat(job.src.path)
            self._resolved[job.id] = await self._resolver(
                dst_end.sftp, job.src.path, job.dst.path,
                src_host=job.src.host, src_is_dir=stat.S_ISDIR(src_attrs.permissions or 0),
            )
        result = await self._copier(
            src_end, job.src.path, dst_end, self._resolved[job.id], _Progress(self, job),
            links=job.options.links, verify=job.options.verify, overwrite=job.options.overwrite,
            chunk_size=self._chunk_size, created_dirs=self._created_dirs[job.id],
        )
        if result is not None and result.failed:
            job.message = f"{result.failed} entries failed"
            self._finish(job, JobState.FAILED)
        else:
            job.bytes_done = job.bytes_total
            self._finish(job, JobState.DONE)

    # ---- state changes -------------------------------------------------

    def _fail(self, job: Job, error: Exception) -> None:
        job.message = str(error) or type(error).__name__
        job.error_kind = "other"
        if isinstance(error, KeyNeedsPassphrase):
            job.error_kind = "passphrase"
        elif isinstance(error, HostKeyError):
            job.error_kind = "hostkey"
        elif isinstance(error, ConnectError):
            job.error_kind = "auth"
        elif isinstance(error, _GaveUp):
            job.error_kind = "connection"
        job.error_alias = getattr(error, "alias", None)
        self._finish(job, JobState.FAILED)

    def _finish(self, job: Job, state: JobState) -> None:
        job.state = state
        job.finished_at = time.time()
        self._emit(job)

    def _emit(self, job: Job, throttle: bool = False) -> None:
        now = time.monotonic()
        if throttle and now - self._last_emit.get(job.id, 0.0) < PROGRESS_INTERVAL:
            return
        self._last_emit[job.id] = now
        self._on_event(JobChanged(dataclasses.replace(job)))


class _GaveUp(Exception):
    """Reconnect attempts ran out."""
