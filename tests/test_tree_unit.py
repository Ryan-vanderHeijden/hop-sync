"""Directory-copy tests with in-memory fakes."""

import stat

import asyncssh
import pytest
from fakes import FakeSFTP, add_file, endpoints

from hopsync.core.events import EntryFailed, EntrySkipped, TransferFinished, TreeProgress
from hopsync.core.tree import classify_entry, copy_tree


def make_endpoints():
    src, dst = endpoints({})
    dst.sftp.dirs.add("/dst")
    return src, dst


def tree_events(events):
    return [e for e in events if isinstance(e, TreeProgress)]


@pytest.mark.parametrize(
    "name, mode, expected",
    [
        ("a.txt", stat.S_IFREG | 0o644, "file"),
        ("d", stat.S_IFDIR | 0o755, "dir"),
        ("l", stat.S_IFLNK | 0o777, "symlink"),
        ("fifo", stat.S_IFIFO | 0o644, "special file"),
        ("sock", stat.S_IFSOCK, "special file"),
        ("dev", stat.S_IFCHR, "special file"),
        ("x.bin.hopsync-part", stat.S_IFREG | 0o644, "hopsync temp file"),
        ("x.bin.hopsync-part.json", stat.S_IFREG | 0o644, "hopsync temp file"),
    ],
)
def test_classify(name, mode, expected):
    assert classify_entry(name, mode) == expected


async def test_copies_tree_and_total_grows():
    src, dst = make_endpoints()
    add_file(src.sftp, "/src/a.txt", b"aaa")
    add_file(src.sftp, "/src/sub/b.txt", b"bbbbb")
    add_file(src.sftp, "/src/sub/deep/c.txt", b"c")
    events = []
    result = await copy_tree(src, "/src", dst, "/dst/src", events.append)

    assert (result.copied, result.skipped, result.failed) == (3, 0, 0)
    assert result.bytes_copied == 9
    assert bytes(dst.sftp.files["/dst/src/sub/deep/c.txt"].data) == b"c"

    progress = tree_events(events)
    found = [e.files_found for e in progress]
    assert found == sorted(found) and found[-1] == 3  # the total only grows
    # walk_complete flips once, and never flips back.
    flags = [e.walk_complete for e in progress]
    assert flags == sorted(flags) and flags[-1] is True
    first_complete = flags.index(True)
    assert progress[first_complete].files_found == 3  # totals are final at that point
    assert progress[-1].files_done == 3 and progress[-1].bytes_done == 9


async def test_walk_complete_event_follows_all_discoveries():
    src, dst = make_endpoints()
    add_file(src.sftp, "/src/a", b"1")
    add_file(src.sftp, "/src/d/b", b"22")
    events = []
    await copy_tree(src, "/src", dst, "/dst/src", events.append)
    progress = tree_events(events)
    complete_at = [e.walk_complete for e in progress].index(True)
    assert all(e.files_found == progress[complete_at].files_found for e in progress[complete_at:])


async def test_directory_metadata_deepest_first_after_children():
    src, dst = make_endpoints()
    add_file(src.sftp, "/src/sub/deep/c.txt", b"c")
    src.sftp.dir_meta.update({"/src": [0o750, 1000], "/src/sub": [0o755, 2000], "/src/sub/deep": [0o700, 3000]})
    await copy_tree(src, "/src", dst, "/dst/src", lambda e: None)

    log = dst.sftp.setstat_log
    dirs_in_order = [p for p in log if p in dst.sftp.dirs]
    assert dirs_in_order == ["/dst/src/sub/deep", "/dst/src/sub", "/dst/src"]
    # every file's metadata was set before any directory's
    assert log.index("/dst/src/sub/deep/c.txt") < log.index("/dst/src/sub/deep")
    assert dst.sftp.dir_meta["/dst/src/sub/deep"] == [0o700, 3000]
    assert dst.sftp.dir_meta["/dst/src"] == [0o750, 1000]


async def test_file_mtime_and_mode_preserved_without_special_bits():
    src, dst = make_endpoints()
    add_file(src.sftp, "/src/run.sh", b"#!", mode=0o4755, mtime=555)
    await copy_tree(src, "/src", dst, "/dst/src", lambda e: None)
    f = dst.sftp.files["/dst/src/run.sh"]
    assert f.mtime == 555 and f.mode == 0o755  # setuid bit dropped


async def test_skips_special_temp_symlink_and_bad_names():
    src, dst = make_endpoints()
    add_file(src.sftp, "/src/ok.txt", b"ok")
    add_file(src.sftp, "/src/left.hopsync-part", b"junk")
    src.sftp.special["/src/pipe"] = stat.S_IFIFO | 0o644
    src.sftp.links["/src/link"] = "ok.txt"
    src.sftp.raw_names["/src"] = [b"bad\xffname"]
    events = []
    result = await copy_tree(src, "/src", dst, "/dst/src", events.append)

    reasons = {e.path: e.reason for e in events if isinstance(e, EntrySkipped)}
    assert reasons["/src/left.hopsync-part"] == "hopsync temp file"
    assert reasons["/src/pipe"] == "special file"
    assert reasons["/src/link"] == "symlink"
    assert any(r == "non-UTF-8 name" for r in reasons.values())
    assert result.copied == 1 and result.skipped == 4
    assert list(dst.sftp.links) == []


async def test_links_flag_copies_symlinks():
    src, dst = make_endpoints()
    add_file(src.sftp, "/src/ok.txt", b"ok")
    src.sftp.links["/src/link"] = "ok.txt"
    dst.sftp.links["/dst/src/link"] = "stale-target"
    dst.sftp.dirs.add("/dst/src")
    # an old link at the path is replaced
    await copy_tree(src, "/src", dst, "/dst/src", lambda e: None, links=True)
    assert dst.sftp.links["/dst/src/link"] == "ok.txt"


async def test_skip_if_identical_and_copy_if_different():
    src, dst = make_endpoints()
    add_file(src.sftp, "/src/same.txt", b"abc", mtime=50)
    add_file(src.sftp, "/src/newer.txt", b"abc", mtime=60)
    add_file(dst.sftp, "/dst/src/same.txt", b"abc", mtime=50)  # identical
    add_file(dst.sftp, "/dst/src/newer.txt", b"abc", mtime=10)  # same size, other mtime
    events = []
    result = await copy_tree(src, "/src", dst, "/dst/src", events.append)

    skipped = [e.path for e in events if isinstance(e, EntrySkipped) and e.reason == "already copied"]
    assert skipped == ["/src/same.txt"]
    assert result.copied == 1 and result.bytes_copied == 3
    finished = [e.src for e in events if isinstance(e, TransferFinished)]
    assert finished == ["/src/newer.txt"]


async def test_unreadable_file_fails_but_others_copy():
    src, dst = make_endpoints()
    add_file(src.sftp, "/src/a.txt", b"aaa")
    add_file(src.sftp, "/src/b.txt", b"bbb")
    src.sftp.files["/src/a.txt"].fail_after = 0
    src.sftp.files["/src/a.txt"].fail_exc = asyncssh.SFTPPermissionDenied("Permission denied")
    result = await copy_tree(src, "/src", dst, "/dst/src", lambda e: None)
    assert (result.copied, result.failed) == (1, 1)
    assert "/dst/src/b.txt" in dst.sftp.files


async def test_lost_connection_stops_the_job():
    src, dst = make_endpoints()
    add_file(src.sftp, "/src/a.txt", b"aaa")
    add_file(src.sftp, "/src/b.txt", b"bbb")
    src.sftp.files["/src/a.txt"].fail_after = 0
    src.sftp.files["/src/a.txt"].fail_exc = asyncssh.SFTPConnectionLost("gone")
    with pytest.raises(asyncssh.SFTPConnectionLost):
        await copy_tree(src, "/src", dst, "/dst/src", lambda e: None)
    assert "/dst/src/b.txt" not in dst.sftp.files


async def test_size_mismatch_is_entry_failure():
    src, dst = make_endpoints()
    dst.sftp.size_lie = 1  # every .part looks one byte too big
    add_file(src.sftp, "/src/a.txt", b"aaa")
    events = []
    result = await copy_tree(src, "/src", dst, "/dst/src", events.append)
    assert result.failed == 1 and result.copied == 0
    assert any(isinstance(e, EntryFailed) for e in events)
    assert result.failures[0][0] == "/src/a.txt"


async def test_source_must_be_directory():
    src, dst = make_endpoints()
    add_file(src.sftp, "/f", b"x")
    with pytest.raises(Exception, match="not a directory"):
        await copy_tree(src, "/f", dst, "/dst/f", lambda e: None)


async def test_directory_metadata_failure_is_per_entry():
    src, dst = make_endpoints()
    add_file(src.sftp, "/src/sub/deep/c.txt", b"c")
    dst.sftp.setstat_errors["/dst/src/sub"] = asyncssh.SFTPPermissionDenied("Permission denied")
    events = []
    result = await copy_tree(src, "/src", dst, "/dst/src", events.append)
    assert result.failed == 1 and result.failures[0][0] == "/dst/src/sub"
    assert "cannot set permissions/time" in result.failures[0][1]
    assert any(isinstance(e, EntryFailed) for e in events)
    # the pass continued: the directories above and below were still done
    assert dst.sftp.setstat_log[-3:] == ["/dst/src/sub/deep", "/dst/src/sub", "/dst/src"]


async def test_directory_metadata_connection_loss_is_fatal():
    src, dst = make_endpoints()
    add_file(src.sftp, "/src/sub/c.txt", b"c")
    dst.sftp.setstat_errors["/dst/src/sub"] = asyncssh.SFTPConnectionLost("gone")
    with pytest.raises(asyncssh.SFTPConnectionLost):
        await copy_tree(src, "/src", dst, "/dst/src", lambda e: None)


async def test_existing_read_only_destination_dir_is_made_writable():
    src, dst = make_endpoints()
    add_file(src.sftp, "/src/ro/a.txt", b"new", mtime=70)
    src.sftp.dir_meta["/src/ro"] = [0o555, 5]
    dst.sftp.dirs.update({"/dst/src", "/dst/src/ro"})
    dst.sftp.dir_meta["/dst/src/ro"] = [0o555, 5]  # left read-only by an earlier run
    await copy_tree(src, "/src", dst, "/dst/src", lambda e: None)
    assert bytes(dst.sftp.files["/dst/src/ro/a.txt"].data) == b"new"
    assert dst.sftp.dir_meta["/dst/src/ro"] == [0o555, 5]  # original mode restored
