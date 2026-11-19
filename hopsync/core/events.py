"""Progress events. The CLI, the API and the tests all consume these."""

from dataclasses import dataclass


@dataclass(frozen=True)
class TransferStarted:
    src: str
    dst: str
    total_bytes: int
    resumed_from: int = 0  # bytes already on the destination (0 = fresh)


@dataclass(frozen=True)
class TransferProgress:
    src: str
    dst: str
    bytes_done: int
    total_bytes: int


@dataclass(frozen=True)
class TransferFinished:
    src: str
    dst: str
    bytes_done: int
    seconds: float


@dataclass(frozen=True)
class Verifying:
    src: str
    dst: str
    method: str  # "size" or "sha256"


@dataclass(frozen=True)
class TreeProgress:
    """Overall progress of a directory copy. The totals grow while the walk runs."""

    src: str
    dst: str
    files_done: int
    files_found: int
    bytes_done: int
    bytes_found: int
    walk_complete: bool  # True once the totals are final


@dataclass(frozen=True)
class EntrySkipped:
    path: str
    reason: str  # symlink, special file, non-UTF-8 name, already copied, hopsync temp file


@dataclass(frozen=True)
class EntryFailed:
    path: str
    message: str
