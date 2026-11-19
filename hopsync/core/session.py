"""Opening SSH connections. Never prompts: secrets arrive as arguments."""

from dataclasses import dataclass
from pathlib import Path

import asyncssh


@dataclass(frozen=True)
class Endpoint:
    """One host: its SSH connection (for commands) and SFTP channel (for files)."""

    alias: str
    conn: asyncssh.SSHClientConnection
    sftp: asyncssh.SFTPClient


class ConnectError(Exception):
    """Base class for connection problems with a readable message."""


class AuthFailed(ConnectError):
    """The server rejected our credentials. Never retried automatically."""


class HostKeyError(ConnectError):
    """The server's host key is unknown or does not match known_hosts."""


class KeyNeedsPassphrase(ConnectError):
    """The private key is encrypted and no (or a wrong) passphrase was given."""


async def connect(
    alias: str,
    *,
    password: str | None = None,
    passphrase: str | None = None,
    config_path: str | Path | None = None,
) -> asyncssh.SSHClientConnection:
    """Open an SSH connection to a host alias from the ssh config."""
    config = Path(config_path) if config_path else Path.home() / ".ssh" / "config"
    config_files = [str(config)] if config.exists() else []

    try:
        # Host key checking stays ON. With known_hosts left at its default,
        # asyncssh uses the config's UserKnownHostsFile (or ~/.ssh/known_hosts).
        # `password` covers both the "password" method and keyboard-interactive:
        # asyncssh answers a single "password" kbdint prompt with it.
        return await asyncssh.connect(
            alias,
            config=config_files,
            password=password,
            passphrase=passphrase,
            keepalive_interval=30,
        )
    except asyncssh.PermissionDenied as exc:
        # Deliberately do not include the password in the message.
        raise AuthFailed(f"authentication to '{alias}' failed ({exc.reason})") from exc
    except asyncssh.HostKeyNotVerifiable as exc:
        raise HostKeyError(
            f"host key for '{alias}' is not trusted ({exc.reason}); "
            "check your known_hosts file"
        ) from exc
    except (asyncssh.KeyEncryptionError, asyncssh.KeyImportError) as exc:
        raise KeyNeedsPassphrase(
            f"the private key for '{alias}' needs a (correct) passphrase"
        ) from exc
