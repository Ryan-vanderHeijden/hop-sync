"""The copy pipeline: read chunks from one SFTP server, write to another."""

import asyncio
import hashlib
import posixpath
import stat
import time
from collections.abc import Callable

import asyncssh

from . import resume
from .errors import ConflictError, TransferError, VerificationFailed
from .events import EntrySkipped, TransferFinished, TransferProgress, TransferStarted, Verifying
from .session import Endpoint
from .verify import remote_sha256

__all__ = ["TransferError", "apply_metadata", "copy_file", "resolve_destination", "stat_or_none"]


async def stat_or_none(sftp, path: str):
    """Return attributes for a path, or None if it does not exist."""
    try:
        return await sftp.stat(path)
    except asyncssh.SFTPNoSuchFile:
        return None


async def resolve_destination(dst_sftp, src_path: str, dst_path: str) -> str:
    """Apply the cp rule: an existing directory receives the file inside it."""
    attrs = await stat_or_none(dst_sftp, dst_path)
    if attrs is not None and stat.S_ISDIR(attrs.permissions or 0):
        if posixpath.basename(src_path) in ("", ".", ".."):
            raise TransferError("cannot tell the source's name; give the destination an explicit name")
        return posixpath.join(dst_path, posixpath.basename(src_path))

    parent = posixpath.dirname(dst_path) or "."
    parent_attrs = await stat_or_none(dst_sftp, parent)
    if parent_attrs is None or not stat.S_ISDIR(parent_attrs.permissions or 0):
        raise TransferError(f"destination directory '{parent}' does not exist")
    return dst_path


async def check_destination(sftp, path: str, size: int, mtime: int, overwrite: bool) -> bool:
    """Look at the destination BEFORE writing anything. The one place for this rule.

    Returns True if the file is already there (same size and mtime), so
    there is nothing to do. Returns False if the path is free, or if it
    holds something different and overwrite is allowed. Raises
    ConflictError if it holds something different and overwrite is off.
    """
    attrs = await stat_or_none(sftp, path)
    if attrs is None:
        return False
    is_regular = stat.S_ISREG(attrs.permissions or 0)
    if is_regular and attrs.size == size and attrs.mtime == mtime:
        return True
    if overwrite:
        return False
    raise ConflictError(path)


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
        await dst_file.write(chunk, offset)
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


async def _finish(dst: Endpoint, dst_path: str, overwrite: bool) -> None:
    """Move the verified .part into place and drop the sidecar."""
    part = resume.part_path(dst_path)
    if overwrite:
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
    overwrite: bool = False,
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

    # Decide about an existing destination before creating anything there.
    if await check_destination(dst.sftp, dst_path, total, src_attrs.mtime or 0, overwrite):
        on_event(EntrySkipped(src_path, "already copied"))
        return total

    # Open the source FIRST: if it cannot be read (e.g. permission denied),
    # we fail before anything has been written on the destination.
    async with src.sftp.open(src_path, "rb") as src_file:
        meta = resume.make_meta(src.alias, src_path, total, src_attrs.mtime or 0)
        offset = await _prepare_part(dst, dst_path, meta, chunk_size)

        started = time.monotonic()
        on_event(TransferStarted(src_path, dst_path, total, resumed_from=offset))

        def report(done: int) -> None:
            on_event(TransferProgress(src_path, dst_path, done, total))

        # Only a fresh transfer can hash while streaming (see _verify).
        hasher = hashlib.sha256() if verify and offset == 0 else None
        mode = "r+b" if offset > 0 else "wb"  # r+b keeps the existing bytes
        async with dst.sftp.open(resume.part_path(dst_path), mode) as dst_file:
            await src_file.seek(offset)
            await _stream(src_file, dst_file, offset, report, hasher, chunk_size, queue_depth)

    try:
        await _verify(src, src_path, dst, dst_path, total, offset, verify, hasher, on_event)
    except VerificationFailed as error:
        # Only bad data is deleted; other failures (e.g. no sha256sum) keep the part.
        await _discard_partial(dst, dst_path)
        raise VerificationFailed(f"{error}; partial file removed, rerun to copy again") from None

    await _finish(dst, dst_path, overwrite)
    await apply_metadata(dst.sftp, dst_path, src_attrs.permissions or 0o644, src_attrs.mtime or 0)
    on_event(TransferFinished(src_path, dst_path, total, time.monotonic() - started))
    return total
