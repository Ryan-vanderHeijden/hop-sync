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


class DiskFullError(TransferError):
    """The destination has too little free space (or quota) for the file."""

    def __init__(self, alias: str, free: int, needed: int) -> None:
        from .units import human_size

        super().__init__(f"destination {alias} is full ({human_size(free)} free, {human_size(needed)} needed)")


class WriteFailedError(TransferError):
    """A write on the destination failed for an unknown reason (maybe full or over quota)."""

    def __init__(self, alias: str) -> None:
        super().__init__(f"write failed on {alias}; the disk may be full or over quota")
