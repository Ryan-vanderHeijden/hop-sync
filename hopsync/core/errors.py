"""Errors shared by the transfer modules."""


class TransferError(Exception):
    """A transfer problem with a readable message."""


class VerificationFailed(TransferError):
    """The copied data is known to be wrong (size or checksum mismatch)."""


CONFLICT_MESSAGE = "exists and differs; not overwriting (use --overwrite to replace)"


class ConflictError(TransferError):
    """The destination already holds a different file and overwrite is off."""

    reason = CONFLICT_MESSAGE  # short form, for per-entry reports that show the path separately

    def __init__(self, path: str) -> None:
        super().__init__(f"'{path}' {CONFLICT_MESSAGE}")
        self.path = path
