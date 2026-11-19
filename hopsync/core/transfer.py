"""The copy pipeline: read chunks from one SFTP server, write to another."""

import asyncio
import posixpath
import stat
import time
from collections.abc import Callable

import asyncssh

from .events import TransferFinished, TransferProgress, TransferStarted


class TransferError(Exception):
    """A transfer problem with a readable message."""


async def _stat_or_none(sftp, path: str):
    """Return attributes for a path, or None if it does not exist."""
    try:
        return await sftp.stat(path)
    except asyncssh.SFTPNoSuchFile:
        return None


async def resolve_destination(dst_sftp, src_path: str, dst_path: str) -> str:
    """Apply the cp rule: an existing directory receives the file inside it."""
    attrs = await _stat_or_none(dst_sftp, dst_path)
    if attrs is not None and stat.S_ISDIR(attrs.permissions or 0):
        return posixpath.join(dst_path, posixpath.basename(src_path))

    parent = posixpath.dirname(dst_path) or "."
    parent_attrs = await _stat_or_none(dst_sftp, parent)
    if parent_attrs is None or not stat.S_ISDIR(parent_attrs.permissions or 0):
        raise TransferError(f"destination directory '{parent}' does not exist")
    return dst_path


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


async def _write_chunks(dst_file, queue: asyncio.Queue, report) -> None:
    """Writer task: drain the queue until the None sentinel."""
    # Writes are parallelised by asyncssh in the same way as reads.
    done = 0
    while (chunk := await queue.get()) is not None:
        await dst_file.write(chunk, done)
        done += len(chunk)
        report(done)


async def copy_file(
    src_sftp,
    src_path: str,
    dst_sftp,
    dst_path: str,
    on_event: Callable,
    *,
    chunk_size: int = 4 * 1024 * 1024,
    queue_depth: int = 4,
) -> int:
    """Copy one file between two SFTP servers. Returns the bytes copied."""
    src_attrs = await src_sftp.stat(src_path)
    if stat.S_ISDIR(src_attrs.permissions or 0):
        raise TransferError(f"'{src_path}' is a directory; directory copy arrives in milestone 2b")
    if not stat.S_ISREG(src_attrs.permissions or 0):
        raise TransferError(f"'{src_path}' is not a regular file")
    total = src_attrs.size or 0

    started = time.monotonic()
    on_event(TransferStarted(src_path, dst_path, total))

    def report(done: int) -> None:
        on_event(TransferProgress(src_path, dst_path, done, total))

    # Memory budget: queue_depth (4) x chunk_size (4 MB) = 16 MB buffered at most.
    queue: asyncio.Queue = asyncio.Queue(maxsize=queue_depth)
    async with src_sftp.open(src_path, "rb") as src_file, dst_sftp.open(dst_path, "wb") as dst_file:
        # TaskGroup: if either task fails, the other is cancelled and the
        # error propagates (inside an ExceptionGroup, unwrapped below).
        try:
            async with asyncio.TaskGroup() as group:
                group.create_task(_read_chunks(src_file, queue, chunk_size))
                group.create_task(_write_chunks(dst_file, queue, report))
        except ExceptionGroup as group_error:
            raise group_error.exceptions[0] from None

    dst_size = (await dst_sftp.stat(dst_path)).size
    if dst_size != total:
        raise TransferError(f"size mismatch: source has {total} bytes, destination has {dst_size}")

    on_event(TransferFinished(src_path, dst_path, total, time.monotonic() - started))
    return total
