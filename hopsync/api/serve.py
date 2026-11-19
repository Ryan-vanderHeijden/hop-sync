"""`hopsync serve`: start the API on 127.0.0.1 and open the browser."""

import asyncio
import getpass
import secrets
import socket
import webbrowser

import uvicorn

from ..core.jobs import JobManager
from ..core.session import KeyNeedsPassphrase
from ..core.sessions import SessionPool
from ..core.sshconfig import list_host_aliases
from .app import create_app
from .hub import EventHub

HOST = "127.0.0.1"  # fixed on purpose: there is no option to listen elsewhere


def pick_free_port() -> int:
    """Ask the OS for a free port (small race window before uvicorn binds it)."""
    with socket.socket() as sock:
        sock.bind((HOST, 0))
        return sock.getsockname()[1]


def build_uvicorn_config(app, port: int) -> uvicorn.Config:
    # access_log off + warning level: request lines (which can carry the
    # token in /auth?token=...) never reach a log.
    return uvicorn.Config(app, host=HOST, port=port, access_log=False, log_level="warning")


async def connect_eagerly(pool: SessionPool, aliases: list[str], passwords: dict) -> None:
    """Connect to the password hosts now, so a wrong password fails at startup."""
    for alias in aliases:
        try:
            await pool.get(alias)
        except KeyNeedsPassphrase:
            passphrase = await asyncio.to_thread(getpass.getpass, f"Passphrase for key of {alias}: ")
            pool.set_credentials(alias, password=passwords.get(alias), passphrase=passphrase)
            await pool.get(alias)  # a second failure is reported once and ends startup


async def run_serve(args) -> int:
    passwords = {alias: getpass.getpass(f"Password for {alias}: ") for alias in args.password_for}
    pool = SessionPool(config_path=args.config)
    for alias, password in passwords.items():
        pool.set_credentials(alias, password=password)
    try:
        await connect_eagerly(pool, args.password_for, passwords)
        hub = EventHub()
        manager = JobManager(pool, hub.publish)
        token, session_secret = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
        port = args.port or pick_free_port()
        app = create_app(manager, pool, list_host_aliases(args.config), token, session_secret, port, hub)
        server = uvicorn.Server(build_uvicorn_config(app, port))

        url = f"http://{HOST}:{port}/auth?token={token}"
        print(f"hopsync is running. Open: {url}", flush=True)
        if not args.no_browser:
            webbrowser.open(url)
        try:
            await server.serve()
        finally:
            await manager.shutdown()  # partial files are kept
    finally:
        await pool.close_all()
    return 0
