"""Tests against the docker hosts (see conftest.py)."""

import asyncio
import hashlib
import os
from contextlib import asynccontextmanager

import asyncssh
import pytest

from hopsync.core.listing import list_dir
from hopsync.core.events import TransferProgress, TransferStarted
from hopsync.cli import main
from hopsync.core.errors import ConflictError, VerificationFailed
from hopsync.core.session import AuthFailed, ConnectError, Endpoint, HostKeyError, connect
from hopsync.core.transfer import copy_file, resolve_destination

pytestmark = pytest.mark.integration


async def open_host(hosts, alias, password=None):
    return await connect(alias, password=password, config_path=hosts.config)


@asynccontextmanager
async def hpc_to_server(hosts):
    """Fresh connections (src=hpc, dst=server); closed on exit."""
    hpc = await open_host(hosts, "hpc", hosts.password)
    server = await open_host(hosts, "server")
    try:
        async with hpc.start_sftp_client() as s, server.start_sftp_client() as d:
            yield Endpoint("hpc", hpc, s), Endpoint("server", server, d)
    finally:
        for conn in (hpc, server):
            conn.close()
            await conn.wait_closed()


async def interrupted_copy(hosts, src_path, dst_path, abort_after=10_000_000):
    """Start a copy and kill the destination connection part-way through."""
    async with hpc_to_server(hosts) as (src, dst):
        def kill_connection(event):
            if isinstance(event, TransferProgress) and event.bytes_done > abort_after:
                dst.conn.abort()

        with pytest.raises(Exception):
            await copy_file(src, src_path, dst, dst_path, kill_connection, chunk_size=1_000_000)


async def exists(conn, path):
    return (await conn.run(f"test -e {path}", check=False)).exit_status == 0


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
        n = await copy_file(Endpoint("src", src, s), "src.bin", Endpoint("dst", dst, d), "dst.bin", events.append)
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
        await copy_file(Endpoint("hpc", hpc, s), "named.bin", Endpoint("server", server, d), target, lambda e: None)
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


async def test_fresh_copy_leaves_no_partial_files(hosts):
    data = os.urandom(3_000_000)
    async with hpc_to_server(hosts) as (src, dst):
        await put_file(src.conn, "fresh.bin", data)
        await copy_file(src, "fresh.bin", dst, "fresh.bin", lambda e: None)
        assert not await exists(dst.conn, "fresh.bin.hopsync-part")
        assert not await exists(dst.conn, "fresh.bin.hopsync-part.json")
        assert await sha256_remote(dst.conn, "fresh.bin") == hashlib.sha256(data).hexdigest()


async def test_verify_on_fresh_copy(hosts):
    data = os.urandom(3_000_000)
    async with hpc_to_server(hosts) as (src, dst):
        await put_file(src.conn, "v.bin", data)
        await copy_file(src, "v.bin", dst, "v.bin", lambda e: None, verify=True)
        assert await sha256_remote(dst.conn, "v.bin") == hashlib.sha256(data).hexdigest()


async def test_resume_after_killed_connection(hosts):
    data = os.urandom(40_000_000)
    async with hpc_to_server(hosts) as (src, dst):
        await put_file(src.conn, "big.bin", data)
    await interrupted_copy(hosts, "big.bin", "big.bin")

    async with hpc_to_server(hosts) as (src, dst):
        assert await exists(dst.conn, "big.bin.hopsync-part")
        assert await exists(dst.conn, "big.bin.hopsync-part.json")
        assert not await exists(dst.conn, "big.bin")

        events = []
        await copy_file(src, "big.bin", dst, "big.bin", events.append, verify=True, chunk_size=1_000_000)
        started = next(e for e in events if isinstance(e, TransferStarted))
        assert started.resumed_from > 0
        assert await sha256_remote(dst.conn, "big.bin") == hashlib.sha256(data).hexdigest()
        assert not await exists(dst.conn, "big.bin.hopsync-part.json")


async def test_changed_source_starts_over(hosts):
    async with hpc_to_server(hosts) as (src, dst):
        await put_file(src.conn, "chg.bin", os.urandom(30_000_000))
    await interrupted_copy(hosts, "chg.bin", "chg.bin", abort_after=5_000_000)

    new_data = os.urandom(20_000_000)  # different size (and mtime)
    async with hpc_to_server(hosts) as (src, dst):
        await put_file(src.conn, "chg.bin", new_data)
        events = []
        await copy_file(src, "chg.bin", dst, "chg.bin", events.append)
        assert next(e for e in events if isinstance(e, TransferStarted)).resumed_from == 0
        assert await sha256_remote(dst.conn, "chg.bin") == hashlib.sha256(new_data).hexdigest()


async def test_corrupted_part_fails_verification(hosts):
    async with hpc_to_server(hosts) as (src, dst):
        await put_file(src.conn, "bad.bin", os.urandom(30_000_000))
    await interrupted_copy(hosts, "bad.bin", "bad.bin", abort_after=5_000_000)

    async with hpc_to_server(hosts) as (src, dst):
        # Test-only: damage 4 bytes inside the already-written prefix.
        await dst.conn.run(
            "printf 'XXXX' | dd of=bad.bin.hopsync-part bs=1 seek=1000 conv=notrunc", check=True
        )
        with pytest.raises(VerificationFailed, match="checksum mismatch"):
            await copy_file(src, "bad.bin", dst, "bad.bin", lambda e: None, verify=True)
        assert not await exists(dst.conn, "bad.bin.hopsync-part")
        assert not await exists(dst.conn, "bad.bin.hopsync-part.json")
        assert not await exists(dst.conn, "bad.bin")


async def test_plain_rename_refuses_to_replace_on_this_sshd(hosts):
    """The no-overwrite default depends on this OpenSSH behaviour."""
    async with hpc_to_server(hosts) as (src, dst):
        await put_file(dst.conn, "ren-a.txt", b"a")
        await put_file(dst.conn, "ren-b.txt", b"b")
        with pytest.raises(asyncssh.SFTPError):
            await dst.sftp.rename("ren-a.txt", "ren-b.txt")
        assert (await dst.conn.run("cat ren-b.txt", check=True)).stdout == "b"


async def cli_copy(hosts, monkeypatch, *args):
    """Run `hopsync copy ...` in-process; returns the exit code."""
    monkeypatch.setattr("getpass.getpass", lambda prompt="": hosts.password)
    argv = ["--config", str(hosts.config), "--password-for", "hpc", "copy", *args]
    return await asyncio.to_thread(main, argv)


async def test_existing_different_file_is_refused(hosts, monkeypatch, capsys):
    mine = b"precious data"
    async with hpc_to_server(hosts) as (src, dst):
        await put_file(dst.conn, "refuse.bin", mine)
        await put_file(src.conn, "refuse.bin", os.urandom(5000))
        with pytest.raises(ConflictError):
            await copy_file(src, "refuse.bin", dst, "refuse.bin", lambda e: None)
        assert await sha256_remote(dst.conn, "refuse.bin") == hashlib.sha256(mine).hexdigest()
        assert not await exists(dst.conn, "refuse.bin.hopsync-part")
        assert not await exists(dst.conn, "refuse.bin.hopsync-part.json")
        code = await cli_copy(hosts, monkeypatch, "hpc:refuse.bin", "server:refuse.bin")
        assert code == 1
        assert "exists and differs" in capsys.readouterr().err
        assert await sha256_remote(dst.conn, "refuse.bin") == hashlib.sha256(mine).hexdigest()


async def test_identical_file_is_already_copied(hosts, monkeypatch, capsys):
    async with hpc_to_server(hosts) as (src, dst):
        await put_file(src.conn, "same.bin", b"same bytes")
        await src.conn.run("touch -d '2020-01-02 03:04:05' same.bin", check=True)
    assert await cli_copy(hosts, monkeypatch, "hpc:same.bin", "server:same.bin") == 0  # first copy
    capsys.readouterr()
    assert await cli_copy(hosts, monkeypatch, "hpc:same.bin", "server:same.bin") == 0
    assert "already copied" in capsys.readouterr().out


async def test_overwrite_flag_replaces_existing_destination(hosts, monkeypatch):
    new_data = os.urandom(2_000_000)
    async with hpc_to_server(hosts) as (src, dst):
        await put_file(dst.conn, "over.bin", b"old contents")
        await put_file(src.conn, "over.bin", new_data)
        await copy_file(src, "over.bin", dst, "over.bin", lambda e: None, on_conflict="overwrite")
        assert await sha256_remote(dst.conn, "over.bin") == hashlib.sha256(new_data).hexdigest()
        await put_file(dst.conn, "over2.bin", b"old contents")
        await src.conn.run("cp over.bin over2.bin", check=True)
        assert await cli_copy(hosts, monkeypatch, "--overwrite", "hpc:over2.bin", "server:over2.bin") == 0
        assert await sha256_remote(dst.conn, "over2.bin") == hashlib.sha256(new_data).hexdigest()
