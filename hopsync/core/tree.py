"""Recursive copy: one job made of many files, walked and copied at once."""

import asyncio
import posixpath
import stat
from collections.abc import Callable
from dataclasses import dataclass, field

import asyncssh

from . import resume
from .errors import ConflictError, TransferError
from .events import EntryFailed, EntrySkipped, TreeProgress
from .session import Endpoint
from .conflict import ConflictInfo, get_resolver
from .explain import explain
from .transfer import apply_metadata, copy_file, find_free_name, lstat_or_none, plan_destination, stat_or_none

# How many discovered files may wait for the copier. Bounded so a huge
# tree cannot fill memory while the (fast) walk runs ahead of the (slow) copy.
WALK_QUEUE_SIZE = 1000

# SFTPError subclasses that mean "the connection is gone", not "this file is bad".
_FATAL_SFTP_ERRORS = (asyncssh.SFTPConnectionLost, asyncssh.SFTPNoConnection)


@dataclass
class TreeResult:
    copied: int = 0
    skipped: int = 0
    failed: int = 0
    bytes_copied: int = 0
    failures: list = field(default_factory=list)  # (path, message) pairs


@dataclass
class _Job:
    """One file (or link) found by the walker, waiting to be copied."""

    src_path: str
    dst_path: str
    size: int
    mtime: int
    link_target: str | None = None  # set for symlinks (--links)


@dataclass
class _Dir:
    """A destination directory, and what to do to it when the job ends."""

    path: str
    mode: int  # the source directory's mode
    mtime: int
    apply_source: bool  # set mode+mtime from the source? (only if we created it, or on_conflict is "overwrite")
    restore_mode: int | None  # original mode, if we had to make it writable


@dataclass
class _State:
    """Everything the walker and the copier share."""

    src: Endpoint
    dst: Endpoint
    src_root: str
    dst_root: str
    on_event: Callable
    links: bool
    on_conflict: object  # a policy name, or the job's resolver function
    created_dirs: set
    result: TreeResult
    queue: asyncio.Queue
    dirs: list = field(default_factory=list)  # _Dir records, parents first
    files_found: int = 0
    bytes_found: int = 0
    files_done: int = 0
    bytes_done: int = 0
    walk_complete: bool = False
    marker: dict = field(default_factory=dict)  # see resume.make_marker

    def emit_progress(self) -> None:
        self.on_event(
            TreeProgress(
                self.src_root, self.dst_root, self.files_done, self.files_found,
                self.bytes_done, self.bytes_found, self.walk_complete,
            )
        )

    def skip(self, path: str, reason: str) -> None:
        self.result.skipped += 1
        self.on_event(EntrySkipped(path, reason))

    def fail(self, path: str, message: str) -> None:
        self.result.failed += 1
        self.result.failures.append((path, message))
        self.on_event(EntryFailed(path, message))


def classify_entry(name: str, mode: int) -> str:
    """Name what an entry is: dir, file, symlink, or a skip reason."""
    # Uses the entry's own (lstat-style) mode, so links are never followed.
    if name == resume.MARKER_NAME or name.endswith(resume.PART_SUFFIX) or name.endswith(resume.PART_SUFFIX + resume.META_SUFFIX):
        return "hopsync temp file"
    if stat.S_ISDIR(mode):
        return "dir"
    if stat.S_ISREG(mode):
        return "file"
    if stat.S_ISLNK(mode):
        return "symlink"
    return "special file"


def _is_per_entry_error(state: "_State", error: Exception) -> bool:
    """True for errors that only affect one entry; the job carries on."""
    if isinstance(error, _FATAL_SFTP_ERRORS):
        return False
    # If either connection is gone, any SFTP error is really a drop: stop, so
    # the job can reconnect instead of marking every remaining file failed.
    if state.src.conn.is_closed() or state.dst.conn.is_closed():
        return False
    return isinstance(error, (TransferError, asyncssh.SFTPError))


def _printable(raw: bytes) -> str:
    return raw.decode("utf-8", "replace")


async def _ensure_dir(state: _State, dst_dir: str, mode: int, mtime: int) -> bool:
    """Make sure a destination directory exists, and record it for the final pass.

    Returns True if this call created the directory."""
    # Directories that already existed are left exactly as found: we only
    # set mode/mtime on directories we created (or when the policy is "overwrite").
    # `created_dirs` remembers directories made earlier by the SAME job (an
    # automatic or manual retry), so they still count as ours. Trade-off: after
    # an interrupted run of the command line tool, a rerun is a new job, so
    # directories made by the first run count as "existing" and keep their
    # default mode/mtime unless the rerun uses --on-conflict overwrite.
    attrs = await stat_or_none(state.dst.sftp, dst_dir)
    restore_mode = None
    created_now = attrs is None
    if attrs is None:
        await state.dst.sftp.mkdir(dst_dir)
        created = True
        state.created_dirs.add(dst_dir)
    elif not stat.S_ISDIR(attrs.permissions or 0):
        raise TransferError(f"'{dst_dir}' exists and is not a directory")
    else:
        created = dst_dir in state.created_dirs
        if (attrs.permissions & 0o300) != 0o300:
            # Not writable by us (e.g. left at 0555). Open it up so we can add
            # files, and remember the original mode to put back at the end.
            restore_mode = attrs.permissions & 0o7777
            await state.dst.sftp.chmod(dst_dir, restore_mode | 0o700)
    # Existing directories get the source's metadata only under "overwrite".
    state.dirs.append(_Dir(dst_dir, mode, mtime, created or state.on_conflict == "overwrite", restore_mode))
    return created_now


async def _walk_dir(state: _State, src_dir: str, dst_dir: str, pending: list) -> None:
    """List one directory; queue its files, push its subdirectories."""
    try:
        # A bytes path makes asyncssh return bytes names, so one badly
        # encoded name cannot break the whole listing.
        entries = await state.src.sftp.readdir(src_dir.encode("utf-8"))
    except asyncssh.SFTPError as error:
        if not _is_per_entry_error(state, error):
            raise
        state.fail(src_dir, f"cannot list directory: {error.reason}")
        return

    jobs = []
    subdirs = []
    for entry in sorted(entries, key=lambda e: e.filename):
        raw = entry.filename
        if raw in (b".", b".."):
            continue
        try:
            name = raw.decode("utf-8")
        except UnicodeDecodeError:
            state.skip(posixpath.join(src_dir, _printable(raw)), "non-UTF-8 name")
            continue
        await _handle_entry(state, src_dir, dst_dir, name, entry.attrs, jobs, subdirs)

    state.files_found += len(jobs)
    state.bytes_found += sum(job.size for job in jobs)
    state.emit_progress()
    for job in jobs:
        await state.queue.put(job)
    # Reversed so the first subdirectory is popped (walked) first.
    pending.extend(reversed(subdirs))


async def _handle_entry(state, src_dir, dst_dir, name, attrs, jobs, subdirs) -> None:
    """Decide what to do with one directory entry."""
    src_path = posixpath.join(src_dir, name)
    dst_path = posixpath.join(dst_dir, name)
    mode = attrs.permissions or 0
    kind = classify_entry(name, mode)

    if kind == "dir":
        try:
            await _ensure_dir(state, dst_path, mode, attrs.mtime or 0)
        except (TransferError, asyncssh.SFTPError) as error:
            if not _is_per_entry_error(state, error):
                raise
            state.fail(src_path, f"cannot create directory: {getattr(error, 'reason', error)}")
            return
        subdirs.append((src_path, dst_path))
    elif kind == "file":
        jobs.append(_Job(src_path, dst_path, attrs.size or 0, attrs.mtime or 0))
    elif kind == "symlink" and state.links:
        target = await _read_link(state, src_path)
        if target is not None:
            jobs.append(_Job(src_path, dst_path, 0, 0, link_target=target))
    else:
        state.skip(src_path, kind)


async def _read_link(state: _State, src_path: str) -> str | None:
    """Read a symlink's target; None (and a failure event) if that fails."""
    try:
        return await state.src.sftp.readlink(src_path)
    except asyncssh.SFTPError as error:
        if not _is_per_entry_error(state, error):
            raise
        state.fail(src_path, f"cannot read link: {error.reason}")
        return None


async def _walker(state: _State, src_attrs) -> None:
    """Walk the source tree depth-first, feeding the copier's queue."""
    created_now = await _ensure_dir(state, state.dst_root, src_attrs.permissions or 0o755, src_attrs.mtime or 0)
    if created_now:
        # Only in a directory WE created: a marker saying "unfinished copy of
        # this source", so a rerun continues here instead of nesting. Never
        # written into an existing directory (that is the user's data).
        await resume.write_marker(state.dst.sftp, state.dst_root, state.marker)
    pending = [(state.src_root, state.dst_root)]  # explicit stack: no recursion limit
    while pending:
        src_dir, dst_dir = pending.pop()
        await _walk_dir(state, src_dir, dst_dir, pending)
    state.walk_complete = True
    state.emit_progress()
    await state.queue.put(None)  # tells the copier there is nothing more


async def _make_link(state: _State, job: _Job) -> str:
    """Create a symlink on the destination. Returns "created", "already" or "skipped"."""
    sftp = state.dst.sftp
    path = job.dst_path
    existing = await lstat_or_none(sftp, path)
    if existing is not None:
        if stat.S_ISLNK(existing.permissions or 0) and await sftp.readlink(path) == job.link_target:
            return "already"
        info = ConflictInfo(path, stat.S_ISDIR(existing.permissions or 0), 0, 0, 0, 0)
        decision = await get_resolver(state.on_conflict)(info)
        if decision == "skip":
            return "skipped"
        if decision == "rename":
            path, _ = await find_free_name(sftp, path, -1, -1)
        elif info.is_dir_at_dst:
            raise TransferError(f"'{path}' is a directory; not replacing it")
        else:
            await sftp.remove(path)
    # asyncssh's symlink() takes (target, link_path) like os.symlink and
    # fixes the argument order that OpenSSH uses on the wire by itself.
    await sftp.symlink(job.link_target, path)
    return "created"


async def _copy_one(state: _State, job: _Job, copy_args: dict) -> None:
    """Copy (or skip) one job, updating the counters."""
    if job.link_target is not None:
        outcome = await _make_link(state, job)
        if outcome == "created":
            state.result.copied += 1
        else:
            state.skip(job.src_path, "already copied" if outcome == "already" else "exists, skipped")
        return
    plan = await plan_destination(state.dst.sftp, job.dst_path, job.size, job.mtime, state.on_conflict)
    if plan.action != "copy":
        state.skip(job.src_path, "already copied" if plan.action == "identical" else "exists, skipped")
        return
    await copy_file(state.src, job.src_path, state.dst, job.dst_path, state.on_event, plan=plan, **copy_args)
    state.result.copied += 1
    state.result.bytes_copied += job.size


async def _copier(state: _State, copy_args: dict) -> None:
    """Take files off the queue and copy them one at a time."""
    while (job := await state.queue.get()) is not None:
        try:
            await _copy_one(state, job, copy_args)
        except (TransferError, asyncssh.SFTPError) as error:
            if not _is_per_entry_error(state, error):
                raise
            state.fail(job.src_path, error.reason if isinstance(error, ConflictError) else explain(error)[1])
        state.files_done += 1
        state.bytes_done += job.size  # failed and skipped files count as processed
        state.emit_progress()


async def _apply_directory_metadata(state: _State) -> None:
    """Final pass over the directories, deepest first."""
    # Writing a child changes its parent's mtime, so parents must be done
    # last. `dirs` lists parents before children; reversed is deepest first.
    for entry in reversed(state.dirs):
        try:
            await _finish_dir(state, entry)
        except (TransferError, asyncssh.SFTPError) as error:
            if not _is_per_entry_error(state, error):
                raise
            reason = getattr(error, "reason", None) or error
            state.fail(entry.path, f"cannot set permissions/time: {reason}")


async def _finish_dir(state: _State, entry: _Dir) -> None:
    if entry.apply_source:
        await apply_metadata(state.dst.sftp, entry.path, entry.mode, entry.mtime)
    elif entry.restore_mode is not None:
        # Leave existing directories exactly as found: put back the mode
        # we changed so we could write into it.
        await state.dst.sftp.chmod(entry.path, entry.restore_mode)


async def copy_tree(
    src: Endpoint,
    src_path: str,
    dst: Endpoint,
    dst_path: str,
    on_event: Callable,
    *,
    links: bool = False,
    verify: bool = False,
    on_conflict="refuse",
    created_dirs: set | None = None,
    chunk_size: int = 4 * 1024 * 1024,
    queue_depth: int = 4,
) -> TreeResult:
    """Copy a directory tree. dst_path is the new top directory (already resolved)."""
    src_attrs = await src.sftp.stat(src_path)
    if not stat.S_ISDIR(src_attrs.permissions or 0):
        raise TransferError(f"'{src_path}' is not a directory")

    state = _State(
        src, dst, src_path, dst_path, on_event, links, on_conflict,
        created_dirs if created_dirs is not None else set(), TreeResult(),
        asyncio.Queue(maxsize=WALK_QUEUE_SIZE),
        marker=resume.make_marker(src.alias, src_path),
    )
    copy_args = {"verify": verify, "chunk_size": chunk_size, "queue_depth": queue_depth}
    # Same TaskGroup pattern as transfer.py: a crash in one task cancels the other.
    try:
        async with asyncio.TaskGroup() as group:
            group.create_task(_walker(state, src_attrs))
            group.create_task(_copier(state, copy_args))
    except ExceptionGroup as group_error:
        raise group_error.exceptions[0] from None

    # Finished without failures: the marker is no longer needed. Removed
    # before the metadata pass because deleting a file changes the directory's mtime.
    if state.result.failed == 0:
        await resume.remove_marker_if_ours(dst.sftp, dst_path, state.marker)
    await _apply_directory_metadata(state)
    return state.result
