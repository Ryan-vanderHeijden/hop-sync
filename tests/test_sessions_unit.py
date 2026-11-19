"""SessionPool and JobManager with fake connections (no network)."""

import asyncio
import dataclasses
from types import SimpleNamespace

import asyncssh
import pytest

from hopsync.core.errors import ConflictError, TransferError
from hopsync.core.jobs import JobChanged, JobEntryEvent, JobManager, JobState
from hopsync.core.events import EntryFailed, TransferProgress, TransferStarted
from hopsync.core.paths import RemotePath
from hopsync.core.session import AuthFailed, KeyNeedsPassphrase
from hopsync.core.sessions import SessionPool


class StubSFTP:
    """Just enough for the manager's source stat (reports a regular file)."""

    async def stat(self, path):
        return SimpleNamespace(permissions=0o100644, size=0, mtime=0)


class FakeConn:
    def __init__(self):
        self.closed = False

    def is_closed(self):
        return self.closed

    def close(self):
        self.closed = True

    async def wait_closed(self):
        pass

    async def start_sftp_client(self):
        await asyncio.sleep(0)
        return StubSFTP()


class FakeConnector:
    def __init__(self, error=None):
        self.calls = []
        self.conns = []
        self.error = error

    async def __call__(self, alias, *, password=None, passphrase=None, config_path=None):
        self.calls.append((alias, password, passphrase))
        await asyncio.sleep(0.01)  # a slow connect, so concurrent callers overlap
        if self.error:
            raise self.error
        conn = FakeConn()
        self.conns.append(conn)
        return conn


# ---- pool -----------------------------------------------------------------


async def test_pool_connects_lazily_and_once_for_concurrent_callers():
    connector = FakeConnector()
    pool = SessionPool(connector=connector)
    assert connector.calls == []
    endpoints = await asyncio.gather(*(pool.get("hpc") for _ in range(5)))
    assert len(connector.calls) == 1
    assert all(e is endpoints[0] for e in endpoints)
    assert endpoints[0].exec_slots is not None


async def test_pool_passes_credentials():
    connector = FakeConnector()
    pool = SessionPool(connector=connector)
    pool.set_credentials("hpc", password="pw", passphrase="pp")
    await pool.get("hpc")
    assert connector.calls == [("hpc", "pw", "pp")]


async def test_pool_reconnects_after_close_and_invalidate():
    connector = FakeConnector()
    pool = SessionPool(connector=connector)
    first = await pool.get("hpc")
    first.conn.close()  # connection dropped
    second = await pool.get("hpc")
    assert second is not first and len(connector.calls) == 2

    pool.invalidate("hpc", only_if=first)  # stale endpoint: must NOT drop the fresh one
    assert await pool.get("hpc") is second
    pool.invalidate("hpc", only_if=second)
    assert await pool.get("hpc") is not second
    assert len(connector.calls) == 3


@pytest.mark.parametrize("error", [AuthFailed("bad", "hpc"), KeyNeedsPassphrase("need", "hpc")])
async def test_pool_remembers_auth_failure_without_contacting_server(error):
    connector = FakeConnector(error=error)
    pool = SessionPool(connector=connector)
    with pytest.raises(type(error)):
        await pool.get("hpc")
    for _ in range(3):
        with pytest.raises(type(error)):
            await pool.get("hpc")
    assert len(connector.calls) == 1  # never contacted again

    connector.error = None
    pool.set_credentials("hpc", password="new")
    await pool.get("hpc")
    assert len(connector.calls) == 2


async def test_pool_close_all():
    connector = FakeConnector()
    pool = SessionPool(connector=connector)
    await pool.get("a")
    await pool.get("b")
    await pool.close_all()
    assert all(c.closed for c in connector.conns)


# ---- manager --------------------------------------------------------------

SRC = RemotePath("hpc", "data")
DST = RemotePath("server", "out")


def make_manager(copier, max_concurrent=2, backoff=(0, 0, 0, 0, 0), resolver=None, connector=None):
    events = []
    pool = SessionPool(connector=connector or FakeConnector())

    async def default_resolver(sftp, src_path, dst_path, **kw):
        return dst_path

    manager = JobManager(
        pool, events.append, max_concurrent, backoff=backoff,
        copier=copier, resolver=resolver or default_resolver,
    )
    return manager, events, pool


def states(events, job_id):
    return [e.job.state for e in events if isinstance(e, JobChanged) and e.job.id == job_id]


async def ok_copier(src, src_path, dst, dst_path, on_event, **kw):
    on_event(TransferStarted(src_path, dst_path, 10))
    on_event(TransferProgress(src_path, dst_path, 10, 10))


async def test_same_host_is_refused():
    manager, _, _ = make_manager(ok_copier)
    with pytest.raises(TransferError, match="same host"):
        manager.add(RemotePath("hpc", "a"), RemotePath("hpc", "b"))


async def test_job_runs_to_done_with_counters():
    manager, events, _ = make_manager(ok_copier)
    job = manager.add(SRC, DST, verify=True)
    assert len(job.id) == 8 and job.options.verify
    await manager.wait_idle()
    final = manager.get(job.id)
    assert final.state is JobState.DONE
    assert (final.bytes_done, final.bytes_total) == (10, 10)
    assert states(events, job.id)[0] is JobState.QUEUED and states(events, job.id)[-1] is JobState.DONE


async def test_never_more_than_max_concurrent_running():
    running = 0
    peak = 0

    async def slow_copier(src, src_path, dst, dst_path, on_event, **kw):
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0.02)
        running -= 1

    manager, events, _ = make_manager(slow_copier, max_concurrent=2)
    for _ in range(4):
        manager.add(SRC, DST)
    await manager.wait_idle()
    assert peak == 2
    # Same thing seen from the events: replay running counts.
    state = {}
    most = 0
    for e in events:
        if isinstance(e, JobChanged):
            state[e.job.id] = e.job.state
            most = max(most, sum(1 for s in state.values() if s is JobState.RUNNING))
    assert most == 2
    assert all(j.state is JobState.DONE for j in manager.list())


async def test_cancel_queued_and_running_jobs():
    gate = asyncio.Event()

    async def blocking_copier(src, src_path, dst, dst_path, on_event, **kw):
        await gate.wait()

    manager, events, _ = make_manager(blocking_copier, max_concurrent=1)
    running = manager.add(SRC, DST)
    queued = manager.add(SRC, DST)
    await asyncio.sleep(0.05)
    assert manager.get(running.id).state is JobState.RUNNING
    assert manager.get(queued.id).state is JobState.QUEUED

    manager.cancel(queued.id)
    assert manager.get(queued.id).state is JobState.CANCELLED
    manager.cancel(running.id)
    await manager.wait_idle()
    assert manager.get(running.id).state is JobState.CANCELLED
    manager.cancel(running.id)  # cancelling a finished job is a no-op
    assert manager.get(running.id).state is JobState.CANCELLED


async def test_retry_from_failed_and_cancelled_only():
    calls = []

    async def flaky(src, src_path, dst, dst_path, on_event, **kw):
        calls.append(1)
        if len(calls) == 1:
            raise ConflictError("x")
        on_event(TransferStarted(src_path, dst_path, 5))

    manager, _, _ = make_manager(flaky)
    job = manager.add(SRC, DST)
    await manager.wait_idle()
    failed = manager.get(job.id)
    assert failed.state is JobState.FAILED and "exists and differs" in failed.message

    retried = manager.retry(job.id)
    assert retried.id == job.id and retried.attempt == 2 and retried.message == ""
    await manager.wait_idle()
    assert manager.get(job.id).state is JobState.DONE
    with pytest.raises(TransferError):
        manager.retry(job.id)  # done jobs cannot be retried


async def test_auto_retry_after_connection_error_succeeds():
    attempts = []

    async def drops_twice(src, src_path, dst, dst_path, on_event, **kw):
        attempts.append(1)
        if len(attempts) <= 2:
            raise asyncssh.SFTPConnectionLost("gone")

    connector = FakeConnector()
    manager, events, _ = make_manager(drops_twice, backoff=(0.01, 0.02, 0.03, 0.04, 0.05), connector=connector)
    job = manager.add(SRC, DST)
    await manager.wait_idle()
    assert manager.get(job.id).state is JobState.DONE
    messages = [e.job.message for e in events if isinstance(e, JobChanged) and e.job.message]
    assert messages == ["reconnecting (attempt 1/5) in 0.01s", "reconnecting (attempt 2/5) in 0.02s"]
    # the job stayed running the whole time
    assert JobState.FAILED not in states(events, job.id)
    # each drop made the pool reconnect (2 hosts x 3 tries)
    assert len(connector.calls) == 6


async def test_gives_up_after_five_reconnects():
    attempts = []

    async def always_drops(src, src_path, dst, dst_path, on_event, **kw):
        attempts.append(1)
        raise ConnectionResetError("reset")

    manager, _, _ = make_manager(always_drops)
    job = manager.add(SRC, DST)
    await manager.wait_idle()
    final = manager.get(job.id)
    assert final.state is JobState.FAILED and final.message.startswith("connection") and final.error_kind == "connection"
    assert len(attempts) == 6  # the first try plus 5 reconnect attempts


async def test_auth_failure_is_not_retried():
    connector = FakeConnector(error=AuthFailed("authentication to 'hpc' failed", "hpc"))
    copier_calls = []

    async def copier(*args, **kw):
        copier_calls.append(1)

    manager, _, _ = make_manager(copier, connector=connector)
    first = manager.add(SRC, DST)
    second = manager.add(SRC, DST)
    await manager.wait_idle()
    for job in (first, second):
        final = manager.get(job.id)
        assert final.state is JobState.FAILED and final.error_kind == "auth"
    assert len(connector.calls) == 1  # the server was contacted exactly once
    assert copier_calls == []


async def test_other_errors_fail_immediately():
    attempts = []

    async def boom(*args, **kw):
        attempts.append(1)
        raise RuntimeError("boom")

    manager, _, _ = make_manager(boom)
    job = manager.add(SRC, DST)
    await manager.wait_idle()
    assert manager.get(job.id).state is JobState.FAILED and attempts == [1]


async def test_tree_with_failed_entries_ends_failed():
    async def tree_copier(src, src_path, dst, dst_path, on_event, **kw):
        on_event(EntryFailed("a.txt", "nope"))
        return SimpleNamespace(failed=2)

    manager, events, _ = make_manager(tree_copier)
    job = manager.add(SRC, DST)
    await manager.wait_idle()
    final = manager.get(job.id)
    assert final.state is JobState.FAILED and final.message == "2 entries failed"
    assert any(isinstance(e, JobEntryEvent) and e.job_id == job.id for e in events)


async def test_progress_events_are_throttled():
    async def chatty(src, src_path, dst, dst_path, on_event, **kw):
        on_event(TransferStarted(src_path, dst_path, 1000))
        for done in range(1, 1001):
            on_event(TransferProgress(src_path, dst_path, done, 1000))

    manager, events, _ = make_manager(chatty)
    manager.add(SRC, DST)
    await manager.wait_idle()
    changes = [e for e in events if isinstance(e, JobChanged)]
    assert len(changes) <= 5  # queued, running, done, plus at most a couple of progress events
    assert manager.get(manager.list()[0].id).bytes_done == 1000  # the counters are still exact


async def test_snapshots_are_independent_copies():
    async def copier(src, src_path, dst, dst_path, on_event, **kw):
        on_event(TransferStarted(src_path, dst_path, 100))

    manager, events, _ = make_manager(copier)
    job = manager.add(SRC, DST)
    await manager.wait_idle()
    first = next(e.job for e in events if isinstance(e, JobChanged))
    assert first.state is JobState.QUEUED and first is not manager.get(job.id)
    assert manager.get(job.id).state is JobState.DONE  # later changes did not touch the snapshot
    assert dataclasses.is_dataclass(first.options) and first.options.__dataclass_params__.frozen
    with pytest.raises(dataclasses.FrozenInstanceError):
        first.src.host = "other"


async def test_destination_is_resolved_once_per_job():
    resolved = []
    seen = []

    async def resolver(sftp, src_path, dst_path, **kw):
        resolved.append(1)
        return "resolved/" + src_path

    async def drops_once(src, src_path, dst, dst_path, on_event, **kw):
        seen.append(dst_path)
        if len(seen) == 1:
            raise asyncssh.SFTPConnectionLost("gone")

    manager, _, _ = make_manager(drops_once, resolver=resolver)
    manager.add(SRC, DST)
    await manager.wait_idle()
    assert len(resolved) == 1 and seen == ["resolved/data", "resolved/data"]


async def test_created_dirs_set_is_shared_across_attempts():
    sets = []

    async def drops_once(src, src_path, dst, dst_path, on_event, *, created_dirs, **kw):
        sets.append(created_dirs)
        created_dirs.add("/dst/x")
        if len(sets) == 1:
            raise asyncssh.SFTPConnectionLost("gone")

    manager, _, _ = make_manager(drops_once)
    manager.add(SRC, DST)
    await manager.wait_idle()
    assert sets[0] is sets[1]


async def test_bytes_transferred_counts_only_what_was_sent():
    from fakes import FakeSFTP, add_file, endpoints
    from hopsync.core.tree import copy_tree

    fake_src, fake_dst = endpoints({})
    fake_dst.sftp.dirs.add("/dst")
    add_file(fake_src.sftp, "/src/a.bin", b"a" * 3000)
    add_file(fake_src.sftp, "/src/b.bin", b"b" * 2000)

    async def tree_copier(src, src_path, dst, dst_path, on_event, **kw):
        return await copy_tree(fake_src, "/src", fake_dst, "/dst/src", on_event, chunk_size=1000)

    manager, events, _ = make_manager(tree_copier)
    first = manager.add(SRC, DST)
    await manager.wait_idle()
    assert manager.get(first.id).bytes_transferred == 5000

    second = manager.add(SRC, DST)  # a rerun: everything is already copied
    await manager.wait_idle()
    rerun = manager.get(second.id)
    assert rerun.state is JobState.DONE
    assert rerun.bytes_transferred == 0 and rerun.bytes_done == rerun.bytes_total == 5000



async def test_bytes_transferred_excludes_resumed_prefix():
    async def copier(src, src_path, dst, dst_path, on_event, **kw):
        on_event(TransferStarted(src_path, dst_path, 1000, resumed_from=600))
        on_event(TransferProgress(src_path, dst_path, 800, 1000))
        on_event(TransferProgress(src_path, dst_path, 1000, 1000))

    manager, _, _ = make_manager(copier)
    job = manager.add(SRC, DST)
    await manager.wait_idle()
    assert manager.get(job.id).bytes_transferred == 400


async def test_sftp_error_on_a_closed_connection_triggers_reconnect():
    attempts = []

    async def copier(src, src_path, dst, dst_path, on_event, **kw):
        attempts.append(1)
        if len(attempts) == 1:
            src.conn.close()  # the link died...
            raise asyncssh.SFTPFailure("Failure")  # ...but we only see a plain error code

    manager, events, _ = make_manager(copier)
    job = manager.add(SRC, DST)
    await manager.wait_idle()
    assert manager.get(job.id).state is JobState.DONE and len(attempts) == 2
