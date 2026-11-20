"""Job queue against the docker hosts: concurrency, cancel, reconnect."""

import asyncio
import subprocess

import pytest
from conftest import COMPOSE, PASSWORD

from hopsync.cli import main
from hopsync.core.events import EntrySkipped
from hopsync.core.jobs import JobChanged, JobManager, JobState
from hopsync.core.paths import RemotePath
from hopsync.core.session import AuthFailed
from hopsync.core.sessions import SessionPool

pytestmark = pytest.mark.integration

BIG = 200_000_000


async def run(conn, command):
    return (await conn.run(command, check=True)).stdout


def docker_exec(service, command):
    """Run a command inside a test container (in a thread: it blocks)."""
    return asyncio.to_thread(
        subprocess.run, [*COMPOSE, "exec", "-T", service, "sh", "-c", command], capture_output=True, text=True
    )


async def kill_sessions(service="hpc"):
    """Drop every SSH session of the test user from the SERVER side."""
    await docker_exec(service, "pkill -9 -f 'tester@' || true")


@pytest.fixture
async def pool(hosts):
    pool = SessionPool(config_path=hosts.config)
    pool.set_credentials("hpc", password=hosts.password)
    yield pool
    await pool.close_all()


async def make_big_file(pool, name):
    hpc = await pool.get("hpc")
    await run(hpc.conn, f"[ -s {name} ] || head -c {BIG} /dev/urandom > {name}")


async def sha(endpoint, path):
    return (await run(endpoint.conn, f"sha256sum {path}")).split()[0]


async def test_cli_runs_four_jobs_two_at_a_time(hosts, monkeypatch, capsys, pool):
    hpc, server = await pool.get("hpc"), await pool.get("server")
    await run(hpc.conn, """
        rm -rf q1.bin q2.bin q3.bin qtree && head -c 3000000 /dev/urandom > q1.bin &&
        head -c 2000000 /dev/urandom > q2.bin && head -c 1000 /dev/urandom > q3.bin &&
        mkdir -p qtree/sub && head -c 500000 /dev/urandom > qtree/a && head -c 700000 /dev/urandom > qtree/sub/b
    """)
    await run(server.conn, "rm -rf qdst && mkdir qdst")
    monkeypatch.setattr("getpass.getpass", lambda prompt="": hosts.password)
    argv = ["--config", str(hosts.config), "--password-for", "hpc", "copy", "--jobs", "2",
            "hpc:q1.bin", "hpc:q2.bin", "hpc:q3.bin", "hpc:qtree", "server:qdst"]
    assert await asyncio.to_thread(main, argv) == 0
    out = capsys.readouterr().out
    assert out.count(" done ") == 4
    for name in ("q1.bin", "q2.bin", "q3.bin"):
        assert await sha(server, f"qdst/{name}") == await sha(hpc, name)
    listing = "cd {d} && find qtree -type f | sort | xargs sha256sum"
    assert await run(server.conn, listing.format(d="qdst")) == await run(hpc.conn, listing.format(d="."))


async def test_cli_multiple_sources_need_an_existing_directory(hosts, monkeypatch, capsys):
    monkeypatch.setattr("getpass.getpass", lambda prompt="": hosts.password)
    argv = ["--config", str(hosts.config), "--password-for", "hpc", "copy",
            "hpc:q1.bin", "hpc:q2.bin", "server:no-such-directory"]
    assert await asyncio.to_thread(main, argv) == 1
    assert "existing directory" in capsys.readouterr().err


async def test_cancel_then_retry_resumes(hosts, pool):
    await make_big_file(pool, "c1.bin")
    events = []
    manager = None
    cancelled = False

    def on_event(event):
        nonlocal cancelled
        events.append(event)
        if isinstance(event, JobChanged) and event.job.bytes_done > 20_000_000 and not cancelled:
            cancelled = True
            manager.cancel(event.job.id)

    manager = JobManager(pool, on_event, chunk_size=1_000_000)
    job = manager.add(RemotePath("hpc", "c1.bin"), RemotePath("server", "c1-copy.bin"))
    await manager.wait_idle()
    assert manager.get(job.id).state is JobState.CANCELLED

    server = await pool.get("server")
    assert (await server.conn.run("test -e c1-copy.bin.hopsync-part", check=False)).exit_status == 0
    assert (await server.conn.run("test -e c1-copy.bin.hopsync-part.json", check=False)).exit_status == 0
    assert (await server.conn.run("test -e c1-copy.bin", check=False)).exit_status != 0
    part_size = int((await run(server.conn, "stat -c %s c1-copy.bin.hopsync-part")).strip())

    events.clear()
    manager.retry(job.id)
    await manager.wait_idle()
    assert manager.get(job.id).state is JobState.DONE and manager.get(job.id).attempt == 2
    first_progress = next(e.job.bytes_done for e in events if isinstance(e, JobChanged) and e.job.bytes_done)
    assert first_progress >= part_size - 1_000_000  # started from the offset, not from 0
    assert await sha(server, "c1-copy.bin") == await sha(await pool.get("hpc"), "c1.bin")


async def test_reconnects_after_server_side_drop(hosts, pool):
    await make_big_file(pool, "r1.bin")
    events = []
    killed = False

    def on_event(event):
        nonlocal killed
        events.append(event)
        if isinstance(event, JobChanged) and event.job.bytes_done > 10_000_000 and not killed:
            killed = True
            asyncio.get_running_loop().create_task(kill_sessions("hpc"))

    manager = JobManager(pool, on_event, chunk_size=1_000_000, backoff=(0.2,) * 5)
    job = manager.add(RemotePath("hpc", "r1.bin"), RemotePath("server", "r1-copy.bin"))
    await manager.wait_idle()

    messages = [e.job.message for e in events if isinstance(e, JobChanged) and e.job.message]
    assert any(m.startswith("reconnecting (attempt 1/5)") for m in messages), messages
    assert manager.get(job.id).state is JobState.DONE
    assert await sha(await pool.get("server"), "r1-copy.bin") == await sha(await pool.get("hpc"), "r1.bin")


async def test_wrong_password_on_reconnect_fails_once(hosts, pool):
    await make_big_file(pool, "w1.bin")
    events = []
    broken = False

    def on_event(event):
        nonlocal broken
        events.append(event)
        if isinstance(event, JobChanged) and event.job.bytes_done > 10_000_000 and not broken:
            broken = True

            async def change_password_and_kill():
                await docker_exec("hpc", "echo tester:another-password | chpasswd")
                await kill_sessions("hpc")

            asyncio.get_running_loop().create_task(change_password_and_kill())

    logs_before = count_failed_logins()
    try:
        manager = JobManager(pool, on_event, chunk_size=1_000_000, backoff=(0.2,) * 5)
        job = manager.add(RemotePath("hpc", "w1.bin"), RemotePath("server", "w1-copy.bin"))
        await manager.wait_idle()
        final = manager.get(job.id)
        assert final.state is JobState.FAILED and final.error_kind == "auth", final
        with pytest.raises(AuthFailed):
            await pool.get("hpc")  # remembered: no further login attempt
        failed_logins = count_failed_logins() - logs_before
        assert failed_logins == 1, f"sshd saw {failed_logins} failed password logins"
    finally:
        await docker_exec("hpc", f"echo tester:{PASSWORD} | chpasswd")


def count_failed_logins() -> int:
    logs = subprocess.run([*COMPOSE, "logs", "hpc"], capture_output=True, text=True).stdout
    return logs.count("Failed password for tester")


async def test_ctrl_c_cancels_jobs_and_keeps_partial_files(hosts, monkeypatch, capsys, pool):
    from hopsync.cli import build_parser, run_copy

    await make_big_file(pool, "k1.bin")
    server = await pool.get("server")
    await run(server.conn, "rm -f k1-copy.bin*")
    monkeypatch.setattr("getpass.getpass", lambda prompt="": hosts.password)
    argv = ["--config", str(hosts.config), "--password-for", "hpc", "copy", "hpc:k1.bin", "server:k1-copy.bin"]
    task = asyncio.create_task(run_copy(build_parser().parse_args(argv)))
    for _ in range(200):  # wait until some data has arrived
        await asyncio.sleep(0.02)
        size = (await server.conn.run("stat -c %s k1-copy.bin.hopsync-part", check=False)).stdout.strip()
        if size and int(size) > 5_000_000:
            break
    task.cancel()  # what Ctrl+C does to the main task
    with pytest.raises(asyncio.CancelledError):
        await task
    assert "cancelled" in capsys.readouterr().out
    assert (await server.conn.run("test -e k1-copy.bin.hopsync-part.json", check=False)).exit_status == 0
    assert (await server.conn.run("test -e k1-copy.bin", check=False)).exit_status != 0
