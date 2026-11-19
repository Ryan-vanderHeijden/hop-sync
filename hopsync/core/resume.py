"""Names and bookkeeping for partial files (<name>.hopsync-part + sidecar)."""

import json

import asyncssh

PART_SUFFIX = ".hopsync-part"
META_SUFFIX = ".json"


def part_path(dst_path: str) -> str:
    """Where the data goes until it has been verified."""
    return dst_path + PART_SUFFIX


def meta_path(dst_path: str) -> str:
    """Sidecar describing the source the .part file was started from."""
    return part_path(dst_path) + META_SUFFIX


def make_meta(source_host: str, source_path: str, size: int, mtime: int) -> dict:
    """What we record about the source when a transfer starts."""
    return {
        "version": 1,
        "source_host": source_host,
        "source_path": source_path,
        "size": size,
        "mtime": mtime,
    }


def resume_offset(saved: dict | None, expected: dict, part_size: int | None, rewind: int) -> int:
    """Decide where to continue. Returns 0 to start over.

    Resume only if the sidecar describes exactly the source we see now
    and the partial file is not bigger than the source. The result is
    `rewind` bytes before the end of the part file (never below 0); see
    _prepare_part for why.
    """
    if saved != expected or part_size is None:
        return 0
    if part_size > expected["size"]:
        return 0
    return max(0, part_size - rewind)


async def read_meta(sftp, dst_path: str) -> dict | None:
    """Load the sidecar; None if it is missing or unreadable."""
    try:
        async with sftp.open(meta_path(dst_path), "rb") as f:
            return json.loads(await f.read())
    except (asyncssh.SFTPError, ValueError):  # no file, or bad JSON/UTF-8
        return None


async def write_meta(sftp, dst_path: str, meta: dict) -> None:
    """Write the sidecar (replacing any old one)."""
    async with sftp.open(meta_path(dst_path), "wb") as f:
        await f.write(json.dumps(meta).encode("utf-8"), 0)


async def part_size(sftp, dst_path: str) -> int | None:
    """Size of the .part file, or None if there is none."""
    try:
        return (await sftp.stat(part_path(dst_path))).size
    except asyncssh.SFTPNoSuchFile:
        return None


async def remove_quietly(sftp, path: str) -> None:
    """Delete a file, ignoring 'does not exist'."""
    try:
        await sftp.remove(path)
    except asyncssh.SFTPNoSuchFile:
        pass
