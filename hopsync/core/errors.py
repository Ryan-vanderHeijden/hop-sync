"""Errors shared by the transfer modules."""


class TransferError(Exception):
    """A transfer problem with a readable message."""


class VerificationFailed(TransferError):
    """The copied data is known to be wrong (size or checksum mismatch)."""
