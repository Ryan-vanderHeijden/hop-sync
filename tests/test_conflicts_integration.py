"""Conflict policies and disk-full handling against the docker hosts."""

import asyncio
import hashlib
import json
import os

import httpx
import pytest
import uvicorn
from test_api_integration import RunningServer, wait_for_job  # noqa: F401 - the server fixture below mirrors it
from websockets.asyncio.client import connect as ws_connect

from hopsync.api.app import create_app
from hopsync.api.hub import EventHub
from hopsync.api.serve import build_uvicorn_config, pick_free_port
from hopsync.core.errors import DiskFullError
from hopsync.core.jobs import JobManager
from hopsync.core.sessions import SessionPool
from hopsync.core.sshconfig import list_host_aliases
from hopsync.core.transfer import copy_file
from hopsync.core.tree import copy_tree
from test_integration import hpc_to_server

pytestmark = pytest.mark.integration


async def run(conn, command):
    return (await conn.run(command, check=True)).stdout


async def sha(conn, path):
    return (await run(conn, f"sha256sum {path}")).split()[0]


@pytest.fixture
async def server(hosts):
    pool = SessionPool(config_path=hosts.config)
    pool.set_credentials("hpc", password=hosts.password)
    await pool.get("hpc")
    hub = EventHub()
    manager = JobManager(pool, hub.publish)
    import secrets

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


async def build_conflicting_tree(hpc, server_end, name):
    """hpc:<name> has a.txt, b.txt, c.txt; server:<name>dst/<name> already has different a.txt and b.txt."""
    await run(hpc.conn, f"""rm -rf {name} && mkdir {name} &&
        echo "new a" > {name}/a.txt && echo "new b!" > {name}/b.txt && echo "new c" > {name}/c.txt""")
    await run(server_end.conn, f"""rm -rf {name}dst && mkdir -p {name}dst/{name} &&
        echo "old a, longer" > {name}dst/{name}/a.txt && echo "old b, longer" > {name}dst/{name}/b.txt""")


async def test_ask_with_skip_and_apply_to_all_over_the_api(hosts, server):
    hpc, server_end = await server.pool.get("hpc"), await server.pool.get("server")
    await build_conflicting_tree(hpc, server_end, "k1")
    cookie = f"hopsync_session={server.session_secret}"
    async with httpx.AsyncClient(base_url=server.origin) as http:
        await http.get(f"/auth?token={server.token}")
        async with ws_connect(server.ws_url, origin=server.origin, additional_headers={"Cookie": cookie}) as ws:
            await ws.recv()  # snapshot
            body = {"src": {"host": "hpc", "path": "k1"}, "dst": {"host": "server", "path": "k1dst"}}
            created = (await http.post("/api/jobs", json=body)).json()  # default policy: ask
            assert created["options"]["on_conflict"] == "ask"

            pending = None
            async with asyncio.timeout(20):
                while pending is None:
                    message = json.loads(await ws.recv())
                    if message["type"] == "job" and message["job"]["pending_conflict"]:
                        pending = message["job"]["pending_conflict"]
            assert pending["path"].endswith("k1/a.txt") and pending["dst_size"] == 14
            answer = {"conflict_id": pending["conflict_id"], "decision": "skip", "apply_to_all": True}
            assert (await http.post(f"/api/jobs/{created['id']}/conflict", json=answer)).status_code == 200

            # b.txt is skipped without asking: the job finishes without a second dialog
            seen_second = False
            async with asyncio.timeout(20):
                while True:
                    message = json.loads(await ws.recv())
                    if message["type"] != "job":
                        continue
                    if message["job"]["pending_conflict"]:
                        seen_second = True
                    if message["job"]["state"] in ("done", "failed"):
                        final = message["job"]
                        break
            assert not seen_second and final["state"] == "done", final

    assert (await run(server_end.conn, "cat k1dst/k1/a.txt")).strip() == "old a, longer"
    assert (await run(server_end.conn, "cat k1dst/k1/b.txt")).strip() == "old b, longer"
    assert (await run(server_end.conn, "cat k1dst/k1/c.txt")).strip() == "new c"


async def test_rename_keeps_both_and_overwrite_replaces(hosts):
    async with hpc_to_server(hosts) as (src, dst):
        await build_conflicting_tree(src, dst, "k2")
        await copy_tree(src, "k2", dst, "k2dst/k2", lambda e: None, on_conflict="rename")
        assert (await run(dst.conn, "cat k2dst/k2/a.txt")).strip() == "old a, longer"
        assert (await run(dst.conn, "cat 'k2dst/k2/a (1).txt'")).strip() == "new a"
        assert (await run(dst.conn, "cat 'k2dst/k2/b (1).txt'")).strip() == "new b!"
        # a rerun does not pile up more copies
        again = await copy_tree(src, "k2", dst, "k2dst/k2", lambda e: None, on_conflict="rename")
        assert again.copied == 0
        assert not (await dst.conn.run("test -e 'k2dst/k2/a (2).txt'", check=False)).exit_status == 0

        await build_conflicting_tree(src, dst, "k3")
        await copy_tree(src, "k3", dst, "k3dst/k3", lambda e: None, on_conflict="overwrite")
        assert (await run(dst.conn, "cat k3dst/k3/a.txt")).strip() == "new a"
        assert (await run(dst.conn, "cat k3dst/k3/b.txt")).strip() == "new b!"


# ---- disk full (server has a 5 MB tmpfs at ~/small) -----------------------------


async def test_single_file_that_cannot_fit_is_refused_up_front(hosts):
    async with hpc_to_server(hosts) as (src, dst):
        await run(src.conn, "[ -s big20.bin ] || head -c 20000000 /dev/urandom > big20.bin")
        assert (await dst.sftp.statvfs("small")).bavail > 0  # statvfs works against this sshd
        with pytest.raises(DiskFullError, match=r"destination server is full \(.* free, 19\.1 MB needed\)"):
            await copy_file(src, "big20.bin", dst, "small/big20.bin", lambda e: None, check_space=True)
        listing = await run(dst.conn, "ls -A small")
        assert listing.strip() == ""  # nothing was created


async def test_tree_with_a_big_file_fails_mid_write_with_a_clear_message(hosts):
    async with hpc_to_server(hosts) as (src, dst):
        await run(src.conn, "rm -rf k4 && mkdir k4 && head -c 20000000 /dev/urandom > k4/big.bin && echo ok > k4/a-small.txt")
        await run(dst.conn, "rm -rf small/k4copy")
        events = []
        result = await copy_tree(src, "k4", dst, "small/k4copy", events.append, chunk_size=1_000_000)
        assert result.failed == 1 and result.copied == 1
        path, message = result.failures[0]
        assert path == "k4/big.bin" and "destination server is full" in message, message
        # the partial file stays, so a rerun can resume once space is freed
        assert (await dst.conn.run("test -e small/k4copy/big.bin.hopsync-part", check=False)).exit_status == 0
        assert (await dst.conn.run("test -e small/k4copy/big.bin", check=False)).exit_status != 0
        await run(dst.conn, "rm -rf small/k4copy")
