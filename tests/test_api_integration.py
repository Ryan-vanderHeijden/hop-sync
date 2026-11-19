"""The real server (uvicorn, in this process) against the docker hosts."""

import asyncio
import hashlib
import json
import os
import secrets

import httpx
import uvicorn
import pytest
from websockets.asyncio.client import connect as ws_connect

from hopsync.api.app import create_app
from hopsync.api.hub import EventHub
from hopsync.api.serve import build_uvicorn_config, pick_free_port
from hopsync.core.jobs import JobManager
from hopsync.core.sessions import SessionPool
from hopsync.core.sshconfig import list_host_aliases

pytestmark = pytest.mark.integration


class RunningServer:
    def __init__(self, port, session_secret, token, pool):
        self.port, self.session_secret, self.token, self.pool = port, session_secret, token, pool
        self.origin = f"http://127.0.0.1:{port}"

    @property
    def ws_url(self):
        return f"ws://127.0.0.1:{self.port}/api/events"


@pytest.fixture
async def server(hosts):
    pool = SessionPool(config_path=hosts.config)
    pool.set_credentials("hpc", password=hosts.password)
    await pool.get("hpc")  # like `serve`: connect eagerly
    hub = EventHub()
    manager = JobManager(pool, hub.publish)
    port, token, session_secret = pick_free_port(), secrets.token_urlsafe(32), secrets.token_urlsafe(32)
    app = create_app(manager, pool, list_host_aliases(hosts.config), token, session_secret, port, hub)
    uvicorn_server = uvicorn.Server(build_uvicorn_config(app, port))
    task = asyncio.create_task(uvicorn_server.serve())
    while not uvicorn_server.started:
        await asyncio.sleep(0.02)
    yield RunningServer(port, session_secret, token, pool)
    uvicorn_server.should_exit = True
    await task
    await manager.shutdown()
    await pool.close_all()


async def run(conn, command):
    return (await conn.run(command, check=True)).stdout


async def wait_for_job(ws, job_id, wanted):
    """Read WebSocket messages until the job reaches one of the wanted states."""
    seen = []
    async with asyncio.timeout(30):
        while True:
            message = json.loads(await ws.recv())
            seen.append(message)
            if message["type"] == "job" and message["job"]["id"] == job_id and message["job"]["state"] in wanted:
                return message["job"], seen


async def test_full_flow_over_http_and_websocket(hosts, server):
    base = server.origin
    async with httpx.AsyncClient(base_url=base) as http:
        assert (await http.get("/api/hosts")).status_code == 401
        auth = await http.get(f"/auth?token={server.token}")
        assert auth.status_code == 303 and "hopsync_session" in http.cookies

        names = {h["alias"]: h for h in (await http.get("/api/hosts")).json()}
        assert names["hpc"]["connected"] is True and set(names) == {"hpc", "server"}

        hpc = await server.pool.get("hpc")
        data = os.urandom(3_000_000)
        async with hpc.sftp.open("api-flow.bin", "wb") as f:
            await f.write(data)
        listing = (await http.get("/api/list", params={"host": "hpc", "path": "."})).json()
        assert any(e["name"] == "api-flow.bin" and e["size"] == len(data) for e in listing["entries"])
        assert listing["abs_path"].startswith("/")

        cookie = f"hopsync_session={server.session_secret}"
        async with ws_connect(server.ws_url, origin=base, additional_headers={"Cookie": cookie}) as ws:
            snapshot = json.loads(await ws.recv())
            assert snapshot["type"] == "snapshot"

            body = {"src": {"host": "hpc", "path": "api-flow.bin"}, "dst": {"host": "server", "path": "api-flow-copy.bin"}}
            created = await http.post("/api/jobs", json=body, headers={"origin": base})
            assert created.status_code == 201
            job, _ = await wait_for_job(ws, created.json()["id"], {"done", "failed"})
            assert job["state"] == "done" and job["bytes_transferred"] == len(data)

            server_end = await server.pool.get("server")
            digest = (await run(server_end.conn, "sha256sum api-flow-copy.bin")).split()[0]
            assert digest == hashlib.sha256(data).hexdigest()

            # Conflict: a different file already sits at the destination.
            await run(server_end.conn, "echo different > api-conflict.bin")
            body["dst"]["path"] = "api-conflict.bin"
            body["on_conflict"] = "refuse"  # the default, "ask", would wait for an answer
            created = await http.post("/api/jobs", json=body)
            job, _ = await wait_for_job(ws, created.json()["id"], {"done", "failed"})
            assert job["state"] == "failed" and "exists and differs" in job["message"]
            assert (await run(server_end.conn, "cat api-conflict.bin")).strip() == "different"

        # A foreign Origin is refused by the real server too.
        with pytest.raises(Exception) as info:
            async with ws_connect(server.ws_url, origin="http://evil.example", additional_headers={"Cookie": cookie}):
                pass
        assert "403" in str(info.value) or "rejected" in str(info.value).lower()
        assert (await http.get("/")).status_code == 200


async def test_wrong_host_header_is_refused_by_the_real_server(server):
    async with httpx.AsyncClient() as http:
        response = await http.get(f"http://127.0.0.1:{server.port}/api/hosts", headers={"host": "evil.example"})
        assert response.status_code == 400
