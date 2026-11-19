"""The copy pipeline: read chunks from one SFTP server, write to another."""

import asyncio
import dataclasses
import hashlib
import posixpath
import stat
import time
from collections.abc import Callable

import asyncssh

from . import resume
from .conflict import ConflictInfo, get_resolver, numbered_name
from .errors import ConflictError, DiskFullError, TransferError, VerificationFailed, WriteFailedError
from .explain import blame
from .events import EntrySkipped, TransferFinished, TransferProgress, TransferStarted, Verifying
from .session import Endpoint
from .verify import remote_sha256

__all__ = ["Plan", "TransferError", "apply_metadata", "copy_file", "plan_destination", "resolve_destination", "stat_or_none"]


async def stat_or_none(sftp, path: str):
    """Return attributes for a path, or None if it does not exist."""
    try:
        return await sftp.stat(path)
    except asyncssh.SFTPNoSuchFile:
        return None


async def resolve_destination(
    dst_sftp, src_path: str, dst_path: str, *, src_host: str | None = None, src_is_dir: bool = False
) -> str:
    """Apply the cp rule: an existing directory receives the file inside it.

    One exception: if the source is a directory and dst_path holds hopsync's
    "incomplete copy" marker for this very source, an earlier run was
    interrupted, so we continue in dst_path itself instead of nesting.
    """
    attrs = await stat_or_none(dst_sftp, dst_path)
    if attrs is not None and stat.S_ISDIR(attrs.permissions or 0):
        if src_is_dir and await resume.read_marker(dst_sftp, dst_path) == resume.make_marker(src_host, src_path):
            return dst_path
        if posixpath.basename(src_path) in ("", ".", ".."):
            raise TransferError("cannot tell the source's name; give the destination an explicit name")
        return posixpath.join(dst_path, posixpath.basename(src_path))

    parent = posixpath.dirname(dst_path) or "."
    parent_attrs = await stat_or_none(dst_sftp, parent)
    if parent_attrs is None or not stat.S_ISDIR(parent_attrs.permissions or 0):
        raise TransferError(f"destination directory '{parent}' does not exist")
    return dst_path


async def lstat_or_none(sftp, path: str):
    """Like stat_or_none but does not follow symlinks."""
    try:
        return await sftp.lstat(path)
    except asyncssh.SFTPNoSuchFile:
        return None


@dataclasses.dataclass(frozen=True)
class Plan:
    """What to do about one destination path, decided BEFORE writing anything."""

    action: str  # "copy", "identical" (already there) or "skipped" (user said skip)
    dst_path: str  # may differ from the requested one ("keep both")
    replace: bool = False  # True: the final rename may replace an existing file


async def find_free_name(sftp, path: str, size: int, mtime: int) -> tuple[str, bool]:
    """First 'name (N).ext' that is free. Returns (path, already_identical).

    A numbered file with the same size and mtime counts as an earlier copy
    of this very source, so rerunning "keep both" does not pile up copies.
    """
    for number in range(1, 10_000):
        candidate = numbered_name(path, number)
        attrs = await lstat_or_none(sftp, candidate)
        if attrs is None:
            return candidate, False
        if stat.S_ISREG(attrs.permissions or 0) and attrs.size == size and attrs.mtime == mtime:
            return candidate, True
    raise TransferError(f"no free name found next to '{path}'")


async def plan_destination(sftp, path: str, size: int, mtime: int, on_conflict) -> Plan:
    """Look at the destination and decide. The one place for the conflict rules.

    Identical files (same size and mtime) are never a conflict. Anything
    else that already exists is handed to the resolver for `on_conflict`
    (a policy name, or an async function such as the job's "ask").
    """
    attrs = await stat_or_none(sftp, path)
    if attrs is None:
        return Plan("copy", path)
    is_dir = stat.S_ISDIR(attrs.permissions or 0)
    if stat.S_ISREG(attrs.permissions or 0) and attrs.size == size and attrs.mtime == mtime:
        return Plan("identical", path)

    info = ConflictInfo(path, is_dir, size, mtime, attrs.size or 0, attrs.mtime or 0)
    decision = await get_resolver(on_conflict)(info)
    if decision == "skip":
        return Plan("skipped", path)
    if decision == "rename":
        new_path, identical = await find_free_name(sftp, path, size, mtime)
        return Plan("identical" if identical else "copy", new_path)
    if is_dir:  # an existing directory is never replaced, whatever was decided
        raise TransferError(f"'{path}' is a directory; not replacing it")
    return Plan("copy", path, replace=True)


async def free_bytes(sftp, directory: str) -> int | None:
    """Free space where `directory` lives, or None if the server cannot say."""
    try:
        info = await sftp.statvfs(directory)  # OpenSSH extension
    except Exception:  # noqa: BLE001 - unsupported or failing: just do not know
        return None
    return (info.bavail or 0) * (info.frsize or info.bsize or 0)


async def _check_space(dst: Endpoint, dst_path: str, total: int) -> None:
    """Refuse early if the destination clearly cannot hold the rest of the file."""
    already = await resume.part_size(dst.sftp, dst_path) or 0
    needed = total - already if already <= total else total
    free = await free_bytes(dst.sftp, posixpath.dirname(dst_path) or ".")
    if free is not None and free < needed:
        raise DiskFullError(dst.alias, free, needed)


async def _write_failure(dst: Endpoint, dst_path: str, remaining: int) -> TransferError:
    """OpenSSH reports a full disk as a plain failure, so ask how much is free."""
    free = await free_bytes(dst.sftp, posixpath.dirname(dst_path) or ".")
    if free is not None and free < remaining:
        return DiskFullError(dst.alias, free, remaining)
    return WriteFailedError(dst.alias)


class _WriteFailed(Exception):
    """Internal: the destination refused a write (the reader is not to blame)."""


async def apply_metadata(sftp, path: str, mode: int, mtime: int) -> None:
    """Set permission bits and times on a remote file or directory."""
    # `& 0o777` keeps only rwx bits: setuid/setgid/sticky are dropped on
    # purpose. Ownership is never touched. atime is set equal to mtime.
    attrs = asyncssh.SFTPAttrs(permissions=mode & 0o777, atime=mtime, mtime=mtime)
    await sftp.setstat(path, attrs)


async def _read_chunks(src_file, queue: asyncio.Queue, chunk_size: int) -> None:
    """Reader task: fill the queue, then put None to mean end-of-file."""
    while True:
        # asyncssh splits a big read into several parallel SFTP requests
        # (up to max_requests of block_size each), so one 4 MB read keeps
        # the link busy instead of waiting a round trip per small block.
        chunk = await src_file.read(chunk_size)
        if not chunk:
            break
        await queue.put(chunk)  # blocks when the queue is full: backpressure
    await queue.put(None)


async def _write_chunks(dst_file, queue: asyncio.Queue, offset: int, report, hasher) -> None:
    """Writer task: drain the queue until the None sentinel."""
    # Writes are parallelised by asyncssh in the same way as reads.
    while (chunk := await queue.get()) is not None:
        try:
            await dst_file.write(chunk, offset)
        except asyncssh.SFTPFailure as error:
            raise _WriteFailed from error
        if hasher is not None:
            hasher.update(chunk)
        offset += len(chunk)
        report(offset)


async def _prepare_part(dst: Endpoint, dst_path: str, expected_meta: dict, chunk_size: int) -> int:
    """Decide resume vs fresh start. Returns the offset to continue from."""
    saved = await resume.read_meta(dst.sftp, dst_path)
    size = await resume.part_size(dst.sftp, dst_path)
    # Each chunk is sent as several blocks at once. If the connection died
    # mid-chunk, some later blocks may have arrived while earlier ones did
    # not, leaving a hole of zeros before the reported end of the file.
    # Every chunk before the last one finished completely, so we step back
    # one chunk and rewrite it instead of trusting the file size.
    offset = resume.resume_offset(saved, expected_meta, size, rewind=chunk_size)
    if offset == 0:
        # Truncate the old part FIRST, then write the sidecar. A crash in
        # between leaves a part with no sidecar, which is never resumed.
        async with dst.sftp.open(resume.part_path(dst_path), "wb"):
            pass
        await resume.write_meta(dst.sftp, dst_path, expected_meta)
    return offset


async def _stream(src_file, dst_file, offset, report, hasher, chunk_size, queue_depth) -> None:
    """Run reader and writer together until the file is copied."""
    # Memory budget: queue_depth (4) x chunk_size (4 MB) = 16 MB buffered at most.
    queue: asyncio.Queue = asyncio.Queue(maxsize=queue_depth)
    # TaskGroup: if either task fails, the other is cancelled and the
    # error propagates (inside an ExceptionGroup, unwrapped here).
    try:
        async with asyncio.TaskGroup() as group:
            group.create_task(_read_chunks(src_file, queue, chunk_size))
            group.create_task(_write_chunks(dst_file, queue, offset, report, hasher))
    except ExceptionGroup as group_error:
        raise group_error.exceptions[0] from None


async def _discard_partial(dst: Endpoint, dst_path: str) -> None:
    """Delete the part and sidecar: their content is known to be bad."""
    await resume.remove_quietly(dst.sftp, resume.part_path(dst_path))
    await resume.remove_quietly(dst.sftp, resume.meta_path(dst_path))


async def _verify(src, src_path, dst, dst_path, total, offset, verify, hasher, on_event) -> None:
    """Check the .part file. Raises TransferError if the data is bad."""
    part = resume.part_path(dst_path)
    size = await resume.part_size(dst.sftp, dst_path)
    if size != total:
        raise VerificationFailed(f"size mismatch: source has {total} bytes, destination has {size}")
    if not verify:
        on_event(Verifying(src_path, dst_path, "size"))
        return

    on_event(Verifying(src_path, dst_path, "sha256"))
    dst_hash = await remote_sha256(dst, part)
    if offset == 0:
        # Fresh transfer: we hashed every byte as it streamed through.
        expected = hasher.hexdigest()
    else:
        # Resumed: our hash misses the first `offset` bytes, so ask the source.
        expected = await remote_sha256(src, src_path)
    if dst_hash != expected:
        raise VerificationFailed("checksum mismatch between source and destination")


async def _finish(dst: Endpoint, dst_path: str, replace: bool) -> None:
    """Move the verified .part into place and drop the sidecar."""
    part = resume.part_path(dst_path)
    if replace:
        try:
            # posix_rename is atomic and replaces an existing file.
            await dst.sftp.posix_rename(part, dst_path)
        except asyncssh.SFTPOpUnsupported:
            await resume.remove_quietly(dst.sftp, dst_path)
            await dst.sftp.rename(part, dst_path)
    else:
        await _rename_without_replacing(dst, part, dst_path)
    await resume.remove_quietly(dst.sftp, resume.meta_path(dst_path))


async def _rename_without_replacing(dst: Endpoint, part: str, dst_path: str) -> None:
    """Plain SFTP rename: OpenSSH refuses to replace an existing target.

    That makes it atomic, so a file that appeared on the destination while
    we were copying is never overwritten.
    """
    try:
        await dst.sftp.rename(part, dst_path)
    except asyncssh.SFTPError:
        if await stat_or_none(dst.sftp, dst_path) is None:
            raise  # some other problem
        await _discard_partial(dst, dst_path)
        raise ConflictError(dst_path) from None


async def copy_file(
    src: Endpoint,
    src_path: str,
    dst: Endpoint,
    dst_path: str,
    on_event: Callable,
    *,
    verify: bool = False,
    on_conflict="refuse",
    plan: Plan | None = None,
    check_space: bool = False,
    chunk_size: int = 4 * 1024 * 1024,
    queue_depth: int = 4,
) -> int:
    """Copy one file between two hosts. Returns the total size of the file.

    Data goes to <dst_path>.hopsync-part and is renamed only after
    verification. If interrupted, the part file is kept so a rerun resumes.
    """
    src_attrs = await src.sftp.stat(src_path)
    if stat.S_ISDIR(src_attrs.permissions or 0):
        raise TransferError(f"'{src_path}' is a directory; directory copy arrives in milestone 2b")
    if not stat.S_ISREG(src_attrs.permissions or 0):
        raise TransferError(f"'{src_path}' is not a regular file")
    total = src_attrs.size or 0

    # Decide about an existing destination before creating anything there
    # (a caller such as copy_tree may already have decided: then `plan` is given).
    if plan is None:
        plan = await plan_destination(dst.sftp, dst_path, total, src_attrs.mtime or 0, on_conflict)
    if plan.action != "copy":
        reason = "already copied" if plan.action == "identical" else "exists, skipped"
        on_event(EntrySkipped(src_path, reason))
        return total
    dst_path = plan.dst_path
    if check_space:
        await _check_space(dst, dst_path, total)

    # Open the source FIRST: if it cannot be read (e.g. permission denied),
    # we fail before anything has been written on the destination.
    written = [0]
    with blame(src.alias, src_path):
        async with src.sftp.open(src_path, "rb") as src_file:
            meta = resume.make_meta(src.alias, src_path, total, src_attrs.mtime or 0)
            try:
                with blame(dst.alias, dst_path):
                    offset = await _prepare_part(dst, dst_path, meta, chunk_size)
            except asyncssh.SFTPFailure as error:  # e.g. no room even for the first bytes
                raise await _write_failure(dst, dst_path, total) from error
            written[0] = offset

            started = time.monotonic()
            on_event(TransferStarted(src_path, dst_path, total, resumed_from=offset))

            def report(done: int) -> None:
                written[0] = done
                on_event(TransferProgress(src_path, dst_path, done, total))

            # Only a fresh transfer can hash while streaming (see _verify).
            hasher = hashlib.sha256() if verify and offset == 0 else None
            mode = "r+b" if offset > 0 else "wb"  # r+b keeps the existing bytes
            try:
                with blame(dst.alias, dst_path):
                    async with dst.sftp.open(resume.part_path(dst_path), mode) as dst_file:
                        await src_file.seek(offset)
                        await _stream(src_file, dst_file, offset, report, hasher, chunk_size, queue_depth)
            except _WriteFailed as error:
                # The .part file stays, so a rerun resumes once space is freed.
                raise await _write_failure(dst, dst_path, total - written[0]) from error.__cause__

    try:
        await _verify(src, src_path, dst, dst_path, total, offset, verify, hasher, on_event)
    except VerificationFailed as error:
        # Only bad data is deleted; other failures (e.g. no sha256sum) keep the part.
        await _discard_partial(dst, dst_path)
        raise VerificationFailed(f"{error}; partial file removed, rerun to copy again") from None

    try:
        with blame(dst.alias, dst_path):
            await _finish(dst, dst_path, plan.replace)
    except asyncssh.SFTPFailure as error:
        raise await _write_failure(dst, dst_path, 0) from error
    await apply_metadata(dst.sftp, dst_path, src_attrs.permissions or 0o644, src_attrs.mtime or 0)
    on_event(TransferFinished(src_path, dst_path, total, time.monotonic() - started))
    return total
