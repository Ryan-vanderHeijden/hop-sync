"""One shared SSH connection (and one SFTP channel) per host, for all jobs."""

import asyncio
from pathlib import Path

from .session import AuthFailed, Endpoint, KeyNeedsPassphrase, connect

# Exec channels (like sha256sum) allowed at once per host.
EXEC_SLOTS = 2


class SessionPool:
    """Opens connections lazily and keeps credentials in memory only."""

    def __init__(self, *, connector=connect, config_path: str | Path | None = None) -> None:
        self._connector = connector
        self._config_path = config_path
        self._credentials: dict[str, dict] = {}
        self._endpoints: dict[str, Endpoint] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._exec_slots: dict[str, asyncio.Semaphore] = {}
        self._auth_errors: dict[str, Exception] = {}

    def set_credentials(self, alias: str, password: str | None = None, passphrase: str | None = None) -> None:
        """Store secrets for a host and allow connecting to it again."""
        self._credentials[alias] = {"password": password, "passphrase": passphrase}
        self._auth_errors.pop(alias, None)

    async def get(self, alias: str) -> Endpoint:
        """Return the shared endpoint for a host, connecting if needed."""
        # One lock per host: callers that arrive while a (re)connect is in
        # progress wait for it instead of each opening their own connection.
        async with self._locks.setdefault(alias, asyncio.Lock()):
            self._raise_remembered_auth_error(alias)
            cached = self._endpoints.get(alias)
            if cached is not None and not cached.conn.is_closed():
                return cached
            self._endpoints.pop(alias, None)
            endpoint = await self._open(alias)
            self._endpoints[alias] = endpoint
            return endpoint

    def _raise_remembered_auth_error(self, alias: str) -> None:
        # A failed login is remembered and re-raised WITHOUT contacting the
        # server: repeated failed logins can lock an HPC account.
        if alias in self._auth_errors:
            raise self._auth_errors[alias]

    async def _open(self, alias: str) -> Endpoint:
        secrets = self._credentials.get(alias, {})
        try:
            conn = await self._connector(
                alias,
                password=secrets.get("password"),
                passphrase=secrets.get("passphrase"),
                config_path=self._config_path,
            )
        except (AuthFailed, KeyNeedsPassphrase) as error:
            self._auth_errors[alias] = error
            raise
        sftp = await conn.start_sftp_client()
        slots = self._exec_slots.setdefault(alias, asyncio.Semaphore(EXEC_SLOTS))
        return Endpoint(alias, conn, sftp, exec_slots=slots)

    def invalidate(self, alias: str, only_if: Endpoint | None = None) -> None:
        """Drop a connection known to be bad (the next get() reconnects).

        With `only_if`, drop it only when it is still that same endpoint:
        another job may already have replaced it with a fresh connection.
        """
        cached = self._endpoints.get(alias)
        if cached is None or (only_if is not None and cached is not only_if):
            return
        del self._endpoints[alias]
        cached.conn.close()

    async def close_all(self) -> None:
        endpoints = list(self._endpoints.values())
        self._endpoints.clear()
        for endpoint in endpoints:
            endpoint.conn.close()
        for endpoint in endpoints:
            await endpoint.conn.wait_closed()
