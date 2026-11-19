"""Parsing of "alias:path" remote specs. Remote paths are always POSIX."""

import posixpath
from dataclasses import dataclass


@dataclass(frozen=True)
class RemotePath:
    host: str  # an alias from ~/.ssh/config
    path: str  # POSIX path; relative paths are relative to the remote home


def normalize_path(path: str) -> str:
    """Clean up a remote path. Empty or "~" means the home directory (".")."""
    # SFTP resolves relative paths against the home directory, so "~" is
    # simply turned into a relative path rather than expanded by us.
    if path == "~":
        path = ""
    elif path.startswith("~/"):
        path = path[2:]
    if path == "":
        return "."
    return posixpath.normpath(path)


def parse_remote(spec: str) -> RemotePath:
    """Parse "alias:path" (split on the first colon)."""
    if ":" not in spec:
        raise ValueError(f"'{spec}' is not of the form ALIAS:PATH")
    host, path = spec.split(":", 1)
    if not host:
        raise ValueError(f"'{spec}' has an empty host alias")
    return RemotePath(host, normalize_path(path))
