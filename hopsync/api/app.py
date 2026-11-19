"""The FastAPI app: REST endpoints, the event WebSocket, and static files."""

import asyncio
import dataclasses
from pathlib import Path

import asyncssh
from fastapi import FastAPI, Query, Request, WebSocket
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.websockets import WebSocketDisconnect

from ..core.errors import TransferError
from ..core.jobs import JobManager
from ..core.listing import list_dir
from ..core.paths import RemotePath, normalize_path
from ..core.session import AuthFailed, HostKeyError, KeyNeedsPassphrase
from ..core.sessions import SessionPool
from .hub import EventHub
from .security import COOKIE_NAME, SecurityMiddleware, same_secret
from .serialize import job_to_json

WEB_DIR = Path(__file__).resolve().parent.parent / "web"
MAX_PATH_LENGTH = 4096


class ApiError(Exception):
    """An error we report to the client as {"error": kind, "message": ...}."""

    def __init__(self, status: int, kind: str, message: str) -> None:
        super().__init__(message)
        self.status, self.kind, self.message = status, kind, message


class RemoteRef(BaseModel):
    host: str
    path: str


class JobRequest(BaseModel):
    src: RemoteRef
    dst: RemoteRef
    verify: bool = False
    links: bool = False
    overwrite: bool = False


def error_response(status: int, kind: str, message: str) -> JSONResponse:
    return JSONResponse({"error": kind, "message": message}, status_code=status)


def clean_path(raw: str) -> str:
    """Validate a path from the client, then normalize it."""
    if len(raw) > MAX_PATH_LENGTH:
        raise ApiError(400, "invalid_path", "path is too long")
    if any(ord(char) < 32 or ord(char) == 127 for char in raw):
        raise ApiError(400, "invalid_path", "path contains control characters")
    return normalize_path(raw)


def create_app(
    manager: JobManager,
    pool: SessionPool,
    aliases: list[str],
    token: str,
    session_secret: str,
    port: int,
    hub: EventHub,
) -> FastAPI:
    """Build the app. `hub` must be the same hub the manager publishes to."""
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    # No CORS middleware, on purpose: without it browsers refuse to let other
    # websites read our responses. Do not add one "to make something work".
    app.add_middleware(SecurityMiddleware, port=port, session_secret=session_secret)

    def check_host(host: str) -> str:
        if host not in aliases:
            raise ApiError(400, "unknown_host", "unknown host")
        return host

    def remote(ref: RemoteRef) -> RemotePath:
        return RemotePath(check_host(ref.host), clean_path(ref.path))

    def find_job(job_id: str):
        try:
            return manager.get(job_id)
        except KeyError:
            raise ApiError(404, "not_found", "no such job") from None

    # ---- sign-in -----------------------------------------------------------

    # The token stays valid for the whole run, so a second tab works. It only
    # ever appears in this computer's terminal and browser history.
    @app.get("/auth")
    async def auth(supplied: str = Query("", alias="token")):
        if not same_secret(supplied, token):
            return error_response(403, "forbidden", "wrong token")
        response = RedirectResponse("/", status_code=303)
        # No `secure` flag: this is plain http on localhost, and browsers
        # would drop a Secure cookie there. HttpOnly keeps scripts away from
        # it and SameSite=Strict keeps other sites from using it.
        response.set_cookie(COOKIE_NAME, session_secret, httponly=True, samesite="strict", path="/")
        return response

    # ---- hosts and listings -------------------------------------------------

    @app.get("/api/hosts")
    async def hosts():
        result = []
        for alias in aliases:
            connected, error_kind = pool.status(alias)
            result.append({"alias": alias, "connected": connected, "error_kind": error_kind})
        return result

    @app.get("/api/list")
    async def list_directory(host: str = Query(...), path: str = Query("")):
        alias = check_host(host)
        clean = clean_path(path)
        endpoint = await pool.get(alias)
        entries = await list_dir(endpoint.sftp, clean)
        abs_path = await endpoint.sftp.realpath(clean)
        return {
            "host": alias,
            "path": clean,
            "abs_path": abs_path,
            "entries": [dataclasses.asdict(entry) for entry in entries],
        }

    # ---- jobs --------------------------------------------------------------

    @app.post("/api/jobs", status_code=201)
    async def create_job(request: JobRequest):
        src, dst = remote(request.src), remote(request.dst)
        try:
            job = manager.add(src, dst, verify=request.verify, links=request.links, overwrite=request.overwrite)
        except TransferError as error:  # same-host copy
            raise ApiError(400, "invalid", str(error)) from None
        return job_to_json(job)

    @app.get("/api/jobs")
    async def list_jobs():
        return [job_to_json(job) for job in manager.list()]

    @app.get("/api/jobs/{job_id}")
    async def get_job(job_id: str):
        return job_to_json(find_job(job_id))

    @app.delete("/api/jobs/{job_id}")
    async def cancel_job(job_id: str):
        find_job(job_id)
        manager.cancel(job_id)
        return job_to_json(manager.get(job_id))

    @app.post("/api/jobs/{job_id}/retry")
    async def retry_job(job_id: str):
        find_job(job_id)
        try:
            return job_to_json(manager.retry(job_id))
        except TransferError as error:
            raise ApiError(409, "state", str(error)) from None

    # ---- events ------------------------------------------------------------

    @app.websocket("/api/events")
    async def events(websocket: WebSocket):
        await websocket.accept()
        # Subscribe and build the snapshot in the same step (no await in
        # between), so every event queued afterwards is newer than the snapshot.
        subscriber = hub.subscribe()
        snapshot = {"type": "snapshot", "jobs": [job_to_json(job) for job in manager.list()]}
        try:
            await websocket.send_json(snapshot)
            await pump_events(websocket, subscriber)
        except WebSocketDisconnect:
            pass
        finally:
            hub.unsubscribe(subscriber)

    # ---- error mapping -----------------------------------------------------

    @app.exception_handler(ApiError)
    async def api_error(request: Request, error: ApiError):
        return error_response(error.status, error.kind, error.message)

    @app.exception_handler(RequestValidationError)
    async def bad_body(request: Request, error: RequestValidationError):
        return error_response(400, "invalid", "invalid request")  # never echo the input back

    @app.exception_handler(StarletteHTTPException)
    async def http_error(request: Request, error: StarletteHTTPException):
        return error_response(error.status_code, "http", str(error.detail))

    @app.exception_handler(asyncssh.SFTPNoSuchFile)
    @app.exception_handler(asyncssh.SFTPNoSuchPath)
    async def no_such_file(request: Request, error: Exception):
        return error_response(404, "not_found", "no such file or directory")

    @app.exception_handler(asyncssh.SFTPPermissionDenied)
    async def permission_denied(request: Request, error: Exception):
        return error_response(403, "permission_denied", "permission denied")

    @app.exception_handler(AuthFailed)
    @app.exception_handler(KeyNeedsPassphrase)
    async def auth_failed(request: Request, error):
        alias = error.alias or "<alias>"
        return error_response(502, "auth", f"restart hopsync with --password-for {alias}")

    @app.exception_handler(HostKeyError)
    async def host_key(request: Request, error: HostKeyError):
        return error_response(502, "hostkey", str(error))

    @app.exception_handler(asyncssh.DisconnectError)
    @app.exception_handler(asyncssh.SFTPConnectionLost)
    @app.exception_handler(asyncssh.SFTPNoConnection)
    @app.exception_handler(OSError)
    @app.exception_handler(TimeoutError)
    async def connection_problem(request: Request, error: Exception):
        return error_response(502, "connection", f"connection problem: {error}")

    @app.exception_handler(asyncssh.SFTPError)
    async def sftp_error(request: Request, error: asyncssh.SFTPError):
        return error_response(502, "sftp", f"SFTP error: {error.reason}")

    @app.exception_handler(Exception)
    async def unexpected(request: Request, error: Exception):
        return error_response(500, "internal", "internal error")  # details stay out of responses

    # Static files last, so the routes above win. The security middleware
    # wraps these too.
    app.mount("/", StaticFiles(directory=WEB_DIR, html=True), name="web")
    return app


async def pump_events(websocket: WebSocket, subscriber) -> None:
    """Send queued events until the client leaves or falls too far behind."""

    async def send_loop() -> None:
        while (message := await subscriber.queue.get()) is not None:
            await websocket.send_json(message)
        await websocket.close(code=1013)  # "try again later": reconnect for a fresh snapshot

    async def watch_for_disconnect() -> None:
        while (await websocket.receive())["type"] != "websocket.disconnect":
            pass  # ignore anything the client sends; we only care that it left

    tasks = [asyncio.create_task(send_loop()), asyncio.create_task(watch_for_disconnect())]
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
    finally:
        for task in tasks:
            task.cancel()
