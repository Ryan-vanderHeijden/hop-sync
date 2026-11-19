"""What to do when a destination file already exists."""

import posixpath
from dataclasses import dataclass

from .errors import ConflictError

# A job's policy. "ask" is turned into a resolver by the job manager.
POLICIES = ("refuse", "ask", "skip", "rename", "overwrite")
# What a resolver may answer.
DECISIONS = ("overwrite", "skip", "rename")


@dataclass(frozen=True)
class ConflictInfo:
    """Everything a resolver (or the user) needs to decide."""

    dst_path: str
    is_dir_at_dst: bool
    src_size: int
    src_mtime: int
    dst_size: int
    dst_mtime: int


def fixed_resolver(policy: str):
    """A resolver that always answers the same way ("refuse" raises)."""
    if policy == "refuse":

        async def refuse(info: ConflictInfo) -> str:
            raise ConflictError(info.dst_path)

        return refuse
    if policy in DECISIONS:

        async def fixed(info: ConflictInfo) -> str:
            return policy

        return fixed
    raise ValueError(f"unknown conflict policy '{policy}'")


def get_resolver(on_conflict):
    """Accept a policy name or a ready-made resolver function."""
    return on_conflict if callable(on_conflict) else fixed_resolver(on_conflict)


def numbered_name(path: str, number: int) -> str:
    """'dir/a.txt' -> 'dir/a (1).txt'.

    Kept simple: the number goes before the LAST extension, so
    'archive.tar.gz' becomes 'archive.tar (1).gz', and a dotfile such as
    '.bashrc' (no extension) becomes '.bashrc (1)'.
    """
    directory, name = posixpath.split(path)
    stem, extension = posixpath.splitext(name)
    return posixpath.join(directory, f"{stem} ({number}){extension}")
