"""Tests against the docker hosts (see conftest.py)."""

import hashlib

import pytest

from hopsync.core.listing import list_dir
from hopsync.core.session import AuthFailed, ConnectError, HostKeyError, connect
from hopsync.core.transfer import copy_file, resolve_destination

pytestmark = pytest.mark.integration


async def open_host(hosts, alias, password=None):
    return await connect(alias, password=password, config_path=hosts.config)


async def sha256_remote(conn, path):
    result = await conn.run(f"sha256sum {path}", check=True)
    return result.stdout.split()[0]


async def put_file(conn, path, data):
    async with conn.start_sftp_client() as sftp:
        async with sftp.open(path, "wb") as f:
            await f.write(data)


@pytest.fixture
async def both(hosts):
    server = await open_host(hosts, "server")
    hpc = await open_host(hosts, "hpc", hosts.password)
    yield server, hpc
    for conn in (server, hpc):
        conn.close()
        await conn.wait_closed()


async def test_list_both_hosts(both):
    for conn in both:
        await put_file(conn, "listed.txt", b"hello")
        async with conn.start_sftp_client() as sftp:
            await sftp.makedirs("somedir", exist_ok=True)
            entries = await list_dir(sftp, ".")
        names = [e.name for e in entries]
        assert "." not in names and ".." not in names
        assert entries[0].is_dir  # directories first
        assert any(e.name == "listed.txt" and e.size == 5 for e in entries)


@pytest.mark.parametrize("direction", ["hpc_to_server", "server_to_hpc"])
async def test_copy_between_hosts(both, direction):
    server, hpc = both
    src, dst = (hpc, server) if direction == "hpc_to_server" else (server, hpc)
    data = bytes(range(256)) * 20_000  # about 5 MB
    await put_file(src, "src.bin", data)
    events = []
    async with src.start_sftp_client() as s, dst.start_sftp_client() as d:
        n = await copy_file(s, "src.bin", d, "dst.bin", events.append)
    assert n == len(data)
    assert events
    assert await sha256_remote(dst, "dst.bin") == hashlib.sha256(data).hexdigest()


async def test_copy_into_existing_directory(both):
    server, hpc = both
    await put_file(hpc, "named.bin", b"data123")
    async with hpc.start_sftp_client() as s, server.start_sftp_client() as d:
        await d.makedirs("intodir", exist_ok=True)
        target = await resolve_destination(d, "named.bin", "intodir")
        assert target == "intodir/named.bin"
        await copy_file(s, "named.bin", d, target, lambda e: None)
        assert await d.exists("intodir/named.bin")


async def test_wrong_password_fails(hosts):
    with pytest.raises(AuthFailed) as info:
        await connect("hpc", password="wrong-password", config_path=hosts.config)
    assert "wrong-password" not in str(info.value)


async def test_missing_password_fails(hosts):
    with pytest.raises(AuthFailed):
        await connect("hpc", config_path=hosts.config)


async def test_unknown_host_key(hosts):
    with pytest.raises(HostKeyError):
        await connect("server", config_path=hosts.config_bad_known_hosts)


def test_errors_share_base():
    assert issubclass(AuthFailed, ConnectError)
