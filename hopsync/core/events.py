"""Progress events. The CLI, the API and the tests all consume these."""

from dataclasses import dataclass


@dataclass(frozen=True)
class TransferStarted:
    src: str
    dst: str
    total_bytes: int


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
