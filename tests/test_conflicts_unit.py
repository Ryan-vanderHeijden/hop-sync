"""Conflict policies, the "ask" flow, explain(), and disk-full handling (all with fakes)."""

import asyncio
import socket
from types import SimpleNamespace

import asyncssh
import pytest
from fakes import FakeFile, FakeSFTP, add_file, endpoints
from test_sessions_unit import DST, SRC, FakeConnector, make_manager, ok_copier

from hopsync.core.conflict import ConflictInfo, fixed_resolver, numbered_name
from hopsync.core.errors import ConflictError, DiskFullError, TransferError, WriteFailedError
from hopsync.core.events import EntrySkipped
from hopsync.core.explain import explain
from hopsync.core.jobs import JobChanged, JobState, NoPendingConflict
from hopsync.core.session import AuthFailed, HostKeyError, KeyNeedsPassphrase
from hopsync.core.transfer import copy_file, find_free_name, plan_destination
from hopsync.core.tree import copy_tree

INFO = ConflictInfo("/x/a.txt", False, 3, 1, 5, 2)


# ---- policies and naming ----------------------------------------------------


async def test_fixed_resolvers():
    assert await fixed_resolver("skip")(INFO) == "skip"
    assert await fixed_resolver("rename")(INFO) == "rename"
    assert await fixed_resolver("overwrite")(INFO) == "overwrite"
    with pytest.raises(ConflictError):
        await fixed_resolver("refuse")(INFO)
    with pytest.raises(ValueError):
        fixed_resolver("ask")  # "ask" needs a job to ask through


@pytest.mark.parametrize(
    "path, number, expected",
    [
        ("/d/a.txt", 1, "/d/a (1).txt"),
        ("a.txt", 2, "a (2).txt"),
        ("/d/noext", 1, "/d/noext (1)"),
        ("/d/.bashrc", 1, "/d/.bashrc (1)"),
        ("/d/archive.tar.gz", 1, "/d/archive.tar (1).gz"),
    ],
)
def test_numbered_name(path, number, expected):
    assert numbered_name(path, number) == expected


async def test_find_free_name_skips_taken_numbers_and_spots_identical_copies():
    sftp = FakeSFTP()
    add_file(sftp, "/d/a.txt", b"old")
    add_file(sftp, "/d/a (1).txt", b"other", mtime=1)
    assert await find_free_name(sftp, "/d/a.txt", 3, 100) == ("/d/a (2).txt", False)
    add_file(sftp, "/d/a (2).txt", b"abc", mtime=100)  # an earlier copy of this very source
    assert await find_free_name(sftp, "/d/a.txt", 3, 100) == ("/d/a (2).txt", True)


async def test_plan_policies():
    sftp = FakeSFTP()
    add_file(sftp, "/d/a.txt", b"old!!", mtime=1)
    plan = await plan_destination(sftp, "/d/a.txt", 3, 100, "skip")
    assert plan.action == "skipped"
    plan = await plan_destination(sftp, "/d/a.txt", 3, 100, "rename")
    assert (plan.action, plan.dst_path, plan.replace) == ("copy", "/d/a (1).txt", False)
    plan = await plan_destination(sftp, "/d/a.txt", 3, 100, "overwrite")
    assert (plan.action, plan.dst_path, plan.replace) == ("copy", "/d/a.txt", True)
    with pytest.raises(ConflictError):
        await plan_destination(sftp, "/d/a.txt", 3, 100, "refuse")
    # identical is never a conflict, under any policy
    add_file(sftp, "/d/same.txt", b"abc", mtime=100)
    assert (await plan_destination(sftp, "/d/same.txt", 3, 100, "refuse")).action == "identical"
    # a directory is never replaced
    sftp.dirs.add("/d/sub")
    with pytest.raises(TransferError, match="directory"):
        await plan_destination(sftp, "/d/sub", 3, 100, "overwrite")
    assert (await plan_destination(sftp, "/d/sub", 3, 100, "skip")).action == "skipped"


async def test_copy_file_skip_and_rename():
    src, dst = endpoints({"a": FakeFile(b"new")}, {"b": FakeFile(b"older data")})
    events = []
    await copy_file(src, "a", dst, "b", events.append, on_conflict="skip")
    assert [e.reason for e in events if isinstance(e, EntrySkipped)] == ["exists, skipped"]
    assert bytes(dst.sftp.files["b"].data) == b"older data"

    await copy_file(src, "a", dst, "b", lambda e: None, on_conflict="rename")
    assert bytes(dst.sftp.files["b"].data) == b"older data"
    assert bytes(dst.sftp.files["b (1)"].data) == b"new"


async def test_tree_policies_and_directory_rule():
    src, dst = endpoints({})
    dst.sftp.dirs.add("/dst")
    add_file(src.sftp, "/src/a.txt", b"new!")
    add_file(src.sftp, "/src/b.txt", b"bbb")
    add_file(dst.sftp, "/dst/src/a.txt", b"old", mtime=1)
    events = []
    result = await copy_tree(src, "/src", dst, "/dst/src", events.append, on_conflict="skip")
    assert (result.copied, result.skipped, result.failed) == (1, 1, 0)
    assert bytes(dst.sftp.files["/dst/src/a.txt"].data) == b"old"
    result = await copy_tree(src, "/src", dst, "/dst/src", lambda e: None, on_conflict="rename")
    assert bytes(dst.sftp.files["/dst/src/a (1).txt"].data) == b"new!"
    # rerunning "keep both" does not make a second copy
    again = await copy_tree(src, "/src", dst, "/dst/src", lambda e: None, on_conflict="rename")
    assert "/dst/src/a (2).txt" not in dst.sftp.files and again.copied == 0


async def test_rename_time_race_is_still_a_conflict_whatever_the_policy():
    src, dst = endpoints({"a": FakeFile(b"new")})
    real_rename = dst.sftp.rename

    async def sneaky(old, new):
        dst.sftp.files[new] = FakeFile(b"someone else")
        await real_rename(old, new)

    dst.sftp.rename = sneaky
    with pytest.raises(ConflictError):
        await copy_file(src, "a", dst, "b", lambda e: None, on_conflict="overwrite")


# ---- the "ask" flow ---------------------------------------------------------


def ask_manager(resolutions=None):
    """A manager whose copier hits N conflicts, asking through the job."""
    asked = []

    async def conflicting(src, src_path, dst, dst_path, on_event, *, on_conflict, **kw):
        for name in ("one.txt", "two.txt"):
            decision = await on_conflict(ConflictInfo(f"/out/{name}", False, 10, 1, 20, 2))
            asked.append((name, decision))

    manager, events, pool = make_manager(conflicting)
    return manager, events, asked


async def wait_for_pending(events, manager, job_id, other_than=None):
    for _ in range(200):
        job = manager.get(job_id)
        if job.pending_conflict and (other_than is None or job.pending_conflict.conflict_id != other_than):
            return job.pending_conflict
        await asyncio.sleep(0.01)
    raise AssertionError("no conflict appeared")


async def test_ask_waits_until_resolved_and_snapshot_shows_it():
    manager, events, asked = ask_manager()
    job = manager.add(SRC, DST, on_conflict="ask")
    pending = await wait_for_pending(events, manager, job.id)
    assert (pending.path, pending.src_size, pending.dst_size) == ("/out/one.txt", 10, 20)
    assert manager.get(job.id).state is JobState.RUNNING
    assert manager.get(job.id).message == "waiting for your decision"
    snapshots = [e.job for e in events if isinstance(e, JobChanged) and e.job.pending_conflict]
    assert snapshots and snapshots[0].pending_conflict.conflict_id == pending.conflict_id

    manager.resolve_conflict(job.id, pending.conflict_id, "skip", apply_to_all=False)
    second = await wait_for_pending(events, manager, job.id, other_than=pending.conflict_id)
    assert second.path == "/out/two.txt" and second.conflict_id != pending.conflict_id
    manager.resolve_conflict(job.id, second.conflict_id, "rename")
    await manager.wait_idle()
    assert asked == [("one.txt", "skip"), ("two.txt", "rename")]
    assert manager.get(job.id).state is JobState.DONE and manager.get(job.id).pending_conflict is None


async def test_apply_to_all_answers_later_conflicts_without_asking():
    manager, events, asked = ask_manager()
    job = manager.add(SRC, DST, on_conflict="ask")
    pending = await wait_for_pending(events, manager, job.id)
    manager.resolve_conflict(job.id, pending.conflict_id, "skip", apply_to_all=True)
    await manager.wait_idle()
    assert asked == [("one.txt", "skip"), ("two.txt", "skip")]
    assert manager.get(job.id).state is JobState.DONE


async def test_stale_or_unknown_conflict_ids_are_refused():
    manager, events, _ = ask_manager()
    job = manager.add(SRC, DST, on_conflict="ask")
    pending = await wait_for_pending(events, manager, job.id)
    with pytest.raises(NoPendingConflict):
        manager.resolve_conflict(job.id, "wrong-id", "skip")
    with pytest.raises(ValueError):
        manager.resolve_conflict(job.id, pending.conflict_id, "explode")
    manager.resolve_conflict(job.id, pending.conflict_id, "skip", True)
    await manager.wait_idle()
    with pytest.raises(NoPendingConflict):
        manager.resolve_conflict(job.id, pending.conflict_id, "skip")  # already answered


async def test_cancelling_a_waiting_job_ends_cancelled():
    manager, events, _ = ask_manager()
    job = manager.add(SRC, DST, on_conflict="ask")
    await wait_for_pending(events, manager, job.id)
    manager.cancel(job.id)
    await manager.wait_idle()
    final = manager.get(job.id)
    assert final.state is JobState.CANCELLED and final.pending_conflict is None
    with pytest.raises(NoPendingConflict):
        manager.resolve_conflict(job.id, "x", "skip")


async def test_unknown_policy_is_rejected():
    manager, _, _ = make_manager(ok_copier)
    with pytest.raises(ValueError):
        manager.add(SRC, DST, on_conflict="maybe")


# ---- explain() --------------------------------------------------------------


def test_explain_cases():
    assert explain(asyncssh.SFTPPermissionDenied("x"), alias="hpc", path="/a") == (
        "permission_denied", "permission denied: /a on hpc")
    assert explain(asyncssh.SFTPNoSuchFile("x"), alias="hpc", path="/a") == ("not_found", "not found: /a on hpc")
    kind, message = explain(AuthFailed("authentication to 'hpc' failed (denied)", "hpc"))
    assert kind == "auth" and message.endswith("restart hopsync with --password-for hpc")
    assert explain(KeyNeedsPassphrase("needs one", "hpc"))[0] == "passphrase"
    assert explain(HostKeyError("host key for 'x' is not trusted")) == ("hostkey", "host key for 'x' is not trusted")
    assert explain(socket.gaierror(-2, "Name or service not known"), alias="hpc") == (
        "connection", "cannot find host hpc (check HostName in your ssh config, or your VPN)")
    kind, message = explain(ConnectionRefusedError(111, "Connection refused"), alias="hpc")
    assert kind == "connection" and message == "cannot reach hpc: Connection refused"
    assert explain(TimeoutError("timed out"), alias="hpc")[1] == "cannot reach hpc: timed out"
    assert explain(asyncssh.SFTPConnectionLost("x"), alias="hpc") == ("connection", "connection to hpc lost")
    assert explain(ConnectionResetError(), alias="hpc")[1] == "connection to hpc lost"
    assert explain(DiskFullError("hpc", 1024, 2048))[0] == "disk_full"
    assert explain(WriteFailedError("hpc")) == ("write_failed", "write failed on hpc; the disk may be full or over quota")
    assert explain(ConflictError("/a"))[0] == "conflict"
    kind, message = explain(RuntimeError("secret-looking detail"), alias="hpc")
    assert (kind, message) == ("other", "unexpected RuntimeError on hpc") and "secret" not in message
    assert explain(asyncssh.SFTPFailure("Failure"), alias="hpc")[1] == "SFTP error on hpc: Failure"


# ---- disk full ---------------------------------------------------------------


def with_statvfs(sftp, free_bytes):
    async def statvfs(path):
        return SimpleNamespace(bavail=free_bytes // 1024, frsize=1024, bsize=1024)

    sftp.statvfs = statvfs
    return sftp


async def test_preflight_refuses_a_single_file_that_cannot_fit():
    src, dst = endpoints({"a": FakeFile(b"x" * 5000)})
    with_statvfs(dst.sftp, 1024)
    with pytest.raises(DiskFullError, match=r"destination server is full \(1\.0 KB free, 4\.9 KB needed\)"):
        await copy_file(src, "a", dst, "b", lambda e: None, check_space=True)
    assert dst.sftp.files == {}  # nothing was created


async def test_preflight_is_skipped_when_unknown_or_not_requested():
    src, dst = endpoints({"a": FakeFile(b"x" * 100)})
    await copy_file(src, "a", dst, "b", lambda e: None, check_space=True)  # fake has no statvfs: carry on
    assert "b" in dst.sftp.files
    src, dst = endpoints({"a": FakeFile(b"x" * 5000)})
    with_statvfs(dst.sftp, 10)
    await copy_file(src, "a", dst, "b", lambda e: None)  # trees do not pre-check
    assert "b" in dst.sftp.files


async def test_mid_write_failure_reports_full_disk_and_keeps_part():
    src, dst = endpoints({"a": FakeFile(b"x" * 5000)}, {"b.hopsync-part": FakeFile(fail_after=1)})
    dst.sftp.files["b.hopsync-part"].fail_exc = asyncssh.SFTPFailure("Failure")
    with_statvfs(dst.sftp, 1024)
    with pytest.raises(DiskFullError, match="destination server is full"):
        await copy_file(src, "a", dst, "b", lambda e: None, chunk_size=1000)
    assert "b.hopsync-part" in dst.sftp.files and "b.hopsync-part.json" in dst.sftp.files and "b" not in dst.sftp.files


async def test_mid_write_failure_without_statvfs_is_generic():
    src, dst = endpoints({"a": FakeFile(b"x" * 5000)}, {"b.hopsync-part": FakeFile(fail_after=1)})
    dst.sftp.files["b.hopsync-part"].fail_exc = asyncssh.SFTPFailure("Failure")
    with pytest.raises(WriteFailedError, match="the disk may be full or over quota"):
        await copy_file(src, "a", dst, "b", lambda e: None, chunk_size=1000)
    assert "b.hopsync-part" in dst.sftp.files


async def test_read_side_failures_are_not_blamed_on_the_destination():
    src, dst = endpoints({"a": FakeFile(b"x" * 5000, fail_after=1)})
    src.sftp.files["a"].fail_exc = asyncssh.SFTPFailure("Failure")
    with pytest.raises(asyncssh.SFTPFailure):
        await copy_file(src, "a", dst, "b", lambda e: None, chunk_size=1000)
