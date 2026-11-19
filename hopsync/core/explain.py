"""One place that turns exceptions into (kind, message) for people.

The CLI, the job manager and the API all use this, so wording is the same
everywhere. Messages never contain secrets: they are built only from the
alias, the path and fixed text.
"""

import contextlib
import errno
import socket

import asyncssh

from .errors import ConflictError, DiskFullError, TransferError, VerificationFailed, WriteFailedError
from .session import AuthFailed, ConnectError, HostKeyError, KeyNeedsPassphrase

_UNREACHABLE_ERRNOS = {errno.ECONNREFUSED, errno.ETIMEDOUT, errno.EHOSTUNREACH, errno.ENETUNREACH}


@contextlib.contextmanager
def blame(alias: str, path: str | None):
    """Remember where an error happened, for `explain` (innermost wins)."""
    try:
        yield
    except Exception as error:
        if not hasattr(error, "hopsync_where"):
            error.hopsync_where = (alias, path)
        raise


def _where(error, alias, path):
    tagged_alias, tagged_path = getattr(error, "hopsync_where", (None, None))
    return alias or tagged_alias or getattr(error, "alias", None), path or tagged_path


def explain(error: BaseException, *, alias: str | None = None, path: str | None = None) -> tuple[str, str]:
    """Return (kind, message). Kinds: auth, passphrase, hostkey, not_found,
    permission_denied, connection, disk_full, write_failed, conflict,
    verification, transfer, other."""
    alias, path = _where(error, alias, path)
    for kind, matches, text in _RULES:
        if matches(error):
            return kind, text(error, alias, path)
    return "other", _generic(error, alias)


def _place(alias, path):
    return (f": {path}" if path else "") + (f" on {alias}" if alias else "")


def _generic(error, alias) -> str:
    on = f" on {alias}" if alias else ""
    if isinstance(error, asyncssh.SFTPError):
        return f"SFTP error{on}: {error.reason}"
    return f"unexpected {type(error).__name__}{on}"


def _is_refused_or_timeout(error) -> bool:
    if isinstance(error, (ConnectionRefusedError, TimeoutError)):
        return True
    return isinstance(error, OSError) and error.errno in _UNREACHABLE_ERRNOS


def _unreachable(error, alias, path) -> str:
    reason = getattr(error, "strerror", None) or str(error) or type(error).__name__
    return f"cannot reach {alias or 'the host'}: {reason}"


def _lost(error, alias, path) -> str:
    return f"connection to {alias} lost" if alias else "connection lost"


def _dropped(error) -> bool:
    if isinstance(error, asyncssh.PermissionDenied):
        return False
    return isinstance(
        error,
        (asyncssh.SFTPConnectionLost, asyncssh.SFTPNoConnection, asyncssh.DisconnectError, ConnectionError),
    )


def _hint(error, alias) -> str:
    return f"{error}; restart hopsync with --password-for {alias or '<alias>'}"


# Order matters: the first rule that matches wins.
_RULES = [
    ("disk_full", lambda e: isinstance(e, DiskFullError), lambda e, a, p: str(e)),
    ("write_failed", lambda e: isinstance(e, WriteFailedError), lambda e, a, p: str(e)),
    ("conflict", lambda e: isinstance(e, ConflictError), lambda e, a, p: str(e)),
    ("verification", lambda e: isinstance(e, VerificationFailed), lambda e, a, p: str(e)),
    ("passphrase", lambda e: isinstance(e, KeyNeedsPassphrase), lambda e, a, p: _hint(e, a)),
    ("auth", lambda e: isinstance(e, AuthFailed), lambda e, a, p: _hint(e, a)),
    ("hostkey", lambda e: isinstance(e, HostKeyError), lambda e, a, p: str(e)),
    ("other", lambda e: isinstance(e, ConnectError), lambda e, a, p: str(e)),
    ("transfer", lambda e: isinstance(e, TransferError), lambda e, a, p: str(e)),
    ("permission_denied", lambda e: isinstance(e, asyncssh.SFTPPermissionDenied),
     lambda e, a, p: f"permission denied{_place(a, p)}"),
    ("not_found", lambda e: isinstance(e, (asyncssh.SFTPNoSuchFile, asyncssh.SFTPNoSuchPath)),
     lambda e, a, p: f"not found{_place(a, p)}"),
    ("connection", lambda e: isinstance(e, socket.gaierror),
     lambda e, a, p: f"cannot find host {a or '<alias>'} (check HostName in your ssh config, or your VPN)"),
    ("connection", _is_refused_or_timeout, _unreachable),
    ("connection", _dropped, _lost),
]
