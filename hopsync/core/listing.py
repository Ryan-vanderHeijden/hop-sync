"""Directory listing over SFTP (never by parsing `ls`)."""

import stat
from dataclasses import dataclass


@dataclass(frozen=True)
class Entry:
    name: str
    size: int
    mtime: int
    is_dir: bool
    is_link: bool


async def list_dir(sftp, path: str) -> list[Entry]:
    """List a remote directory: directories first, then by name."""
    entries = []
    for item in await sftp.readdir(path):
        name = item.filename
        if isinstance(name, bytes):
            name = name.decode("utf-8", "replace")
        if name in (".", ".."):
            continue
        attrs = item.attrs
        mode = attrs.permissions or 0
        entries.append(
            Entry(
                name=name,
                size=attrs.size or 0,
                mtime=attrs.mtime or 0,
                is_dir=stat.S_ISDIR(mode),
                is_link=stat.S_ISLNK(mode),
            )
        )
    entries.sort(key=lambda e: (not e.is_dir, e.name))
    return entries
