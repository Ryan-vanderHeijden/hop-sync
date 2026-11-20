"""Directory-copy tests with in-memory fakes."""

import json
import stat

import asyncssh
import pytest
from fakes import FakeSFTP, add_file, endpoints

from hopsync.core.events import EntryFailed, EntrySkipped, TransferFinished, TreeProgress
from hopsync.core.transfer import resolve_destination
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
    await copy_tree(src, "/src", dst, "/dst/src", lambda e: None, links=True)
    assert dst.sftp.links["/dst/src/link"] == "ok.txt"


async def test_existing_links_same_target_skipped_other_refused_or_replaced():
    src, dst = make_endpoints()
    add_file(src.sftp, "/src/ok.txt", b"ok")
    src.sftp.links["/src/same"] = "ok.txt"
    src.sftp.links["/src/other"] = "ok.txt"
    dst.sftp.dirs.add("/dst/src")
    dst.sftp.links["/dst/src/same"] = "ok.txt"
    dst.sftp.links["/dst/src/other"] = "different"
    events = []
    result = await copy_tree(src, "/src", dst, "/dst/src", events.append, links=True)
    assert [e.path for e in events if isinstance(e, EntrySkipped) and e.reason == "already copied"] == ["/src/same"]
    assert [path for path, _ in result.failures] == ["/src/other"]
    assert dst.sftp.links["/dst/src/other"] == "different"  # untouched

    result = await copy_tree(src, "/src", dst, "/dst/src", lambda e: None, links=True, on_conflict="overwrite")
    assert result.failed == 0
    assert dst.sftp.links["/dst/src/other"] == "ok.txt"


async def test_skip_if_identical_and_copy_if_different():
    src, dst = make_endpoints()
    add_file(src.sftp, "/src/same.txt", b"abc", mtime=50)
    add_file(src.sftp, "/src/newer.txt", b"abc", mtime=60)
    add_file(dst.sftp, "/dst/src/same.txt", b"abc", mtime=50)  # identical
    add_file(dst.sftp, "/dst/src/newer.txt", b"abc", mtime=10)  # same size, other mtime
    events = []
    result = await copy_tree(src, "/src", dst, "/dst/src", events.append, on_conflict="overwrite")

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


async def test_existing_read_only_destination_dir_is_restored_to_original_mode():
    src, dst = make_endpoints()
    add_file(src.sftp, "/src/ro/a.txt", b"new", mtime=70)
    src.sftp.dir_meta["/src/ro"] = [0o755, 9]
    dst.sftp.dirs.update({"/dst/src", "/dst/src/ro"})
    dst.sftp.dir_meta["/dst/src/ro"] = [0o555, 5]  # existing, read-only, not made by us
    await copy_tree(src, "/src", dst, "/dst/src", lambda e: None)
    assert bytes(dst.sftp.files["/dst/src/ro/a.txt"].data) == b"new"
    assert dst.sftp.dir_meta["/dst/src/ro"] == [0o555, 5]  # original mode restored


async def test_conflicting_file_is_refused_without_overwrite():
    src, dst = make_endpoints()
    add_file(src.sftp, "/src/a.txt", b"new")
    add_file(dst.sftp, "/dst/src/a.txt", b"old", mtime=1)
    result = await copy_tree(src, "/src", dst, "/dst/src", lambda e: None)
    assert (result.copied, result.failed) == (0, 1)
    assert result.failures == [("/src/a.txt", "exists and differs; not overwriting (use --overwrite to replace)")]
    assert bytes(dst.sftp.files["/dst/src/a.txt"].data) == b"old"
    assert set(dst.sftp.files) == {"/dst/src/a.txt"}  # no .part or sidecar


async def test_existing_directories_keep_their_metadata_unless_overwrite():
    src, dst = make_endpoints()
    add_file(src.sftp, "/src/sub/new.txt", b"n")
    src.sftp.dir_meta.update({"/src": [0o700, 900], "/src/sub": [0o700, 900]})
    dst.sftp.dirs.update({"/dst/src", "/dst/src/sub"})
    dst.sftp.dir_meta.update({"/dst/src": [0o750, 10], "/dst/src/sub": [0o750, 10]})

    await copy_tree(src, "/src", dst, "/dst/src", lambda e: None)
    assert dst.sftp.dir_meta["/dst/src"] == [0o750, 10]  # untouched
    assert dst.sftp.dir_meta["/dst/src/sub"] == [0o750, 10]

    dst.sftp.files.clear()
    await copy_tree(src, "/src", dst, "/dst/src", lambda e: None, on_conflict="overwrite")
    assert dst.sftp.dir_meta["/dst/src"] == [0o700, 900]  # source metadata applied
    assert dst.sftp.dir_meta["/dst/src/sub"] == [0o700, 900]


async def test_created_directories_get_source_metadata_without_overwrite():
    src, dst = make_endpoints()
    add_file(src.sftp, "/src/sub/new.txt", b"n")
    src.sftp.dir_meta["/src/sub"] = [0o700, 900]
    await copy_tree(src, "/src", dst, "/dst/src", lambda e: None)
    assert dst.sftp.dir_meta["/dst/src/sub"] == [0o700, 900]


# ---- marker for unfinished tree copies ------------------------------------

MARKER = "/dst/src/.hopsync-incomplete.json"


def put_marker(sftp, top, host="hpc", path="/src"):
    marker = json.dumps({"version": 1, "source_host": host, "source_path": path}).encode()
    add_file(sftp, top + "/.hopsync-incomplete.json", marker)


async def test_resolve_continues_into_directory_with_matching_marker():
    dst = FakeSFTP(dirs={"newname"})
    put_marker(dst, "newname", path="srcdir")
    kw = {"src_host": "hpc", "src_is_dir": True}
    assert await resolve_destination(dst, "srcdir", "newname", **kw) == "newname"


@pytest.mark.parametrize(
    "marker_host, marker_path, src_is_dir",
    [("other", "srcdir", True), ("hpc", "elsewhere", True), ("hpc", "srcdir", False)],
)
async def test_resolve_nests_when_marker_does_not_match(marker_host, marker_path, src_is_dir):
    dst = FakeSFTP(dirs={"newname"})
    put_marker(dst, "newname", host=marker_host, path=marker_path)
    result = await resolve_destination(dst, "srcdir", "newname", src_host="hpc", src_is_dir=src_is_dir)
    assert result == "newname/srcdir"


async def test_resolve_nests_without_or_with_unreadable_marker():
    dst = FakeSFTP(dirs={"newname"})
    kw = {"src_host": "hpc", "src_is_dir": True}
    assert await resolve_destination(dst, "srcdir", "newname", **kw) == "newname/srcdir"
    add_file(dst, "newname/.hopsync-incomplete.json", b"{not json")
    assert await resolve_destination(dst, "srcdir", "newname", **kw) == "newname/srcdir"


async def test_marker_is_written_in_created_top_dir_and_removed_when_clean():
    src, dst = make_endpoints()
    add_file(src.sftp, "/src/a.txt", b"aaa")
    seen = []

    def watch(event):
        if isinstance(event, TransferFinished):
            seen.append(MARKER in dst.sftp.files)  # present while the copy runs

    await copy_tree(src, "/src", dst, "/dst/src", watch)
    assert seen == [True]
    assert MARKER not in dst.sftp.files


async def test_marker_is_kept_when_entries_fail_and_never_made_in_existing_dirs():
    src, dst = make_endpoints()
    add_file(src.sftp, "/src/a.txt", b"new")
    add_file(dst.sftp, "/dst/src/a.txt", b"old", mtime=1)  # existing dir: no marker at all
    await copy_tree(src, "/src", dst, "/dst/src", lambda e: None)
    assert MARKER not in dst.sftp.files

    # a failing entry in a directory hopsync creates itself
    src, dst = make_endpoints()
    add_file(src.sftp, "/src/a.txt", b"aaa")
    src.sftp.files["/src/a.txt"].fail_after = 0
    src.sftp.files["/src/a.txt"].fail_exc = asyncssh.SFTPPermissionDenied("denied")
    result = await copy_tree(src, "/src", dst, "/dst/src", lambda e: None)
    assert result.failed == 1
    assert MARKER in dst.sftp.files  # kept: a rerun resumes into the same place


async def test_no_marker_in_existing_directory():
    src, dst = make_endpoints()
    add_file(src.sftp, "/src/a.txt", b"aaa")
    dst.sftp.dirs.add("/dst/src")
    seen = []
    await copy_tree(src, "/src", dst, "/dst/src", lambda e: seen.append(MARKER in dst.sftp.files))
    assert not any(seen)


async def test_marker_in_source_is_not_copied():
    src, dst = make_endpoints()
    add_file(src.sftp, "/src/a.txt", b"aaa")
    add_file(src.sftp, "/src/.hopsync-incomplete.json", b"{}")
    events = []
    await copy_tree(src, "/src", dst, "/dst/src", events.append)
    assert [e.reason for e in events if isinstance(e, EntrySkipped)] == ["hopsync temp file"]


# ---- connection drops inside a tree ----------------------------------------


async def test_sftp_error_with_closed_connection_is_fatal():
    src, dst = make_endpoints()
    add_file(src.sftp, "/src/a.txt", b"aaa")
    add_file(src.sftp, "/src/b.txt", b"bbb")
    f = src.sftp.files["/src/a.txt"]
    f.fail_after = 0
    f.fail_exc = asyncssh.SFTPFailure("Failure")  # an ordinary error code...
    src.conn.closed = True  # ...but the connection is gone
    with pytest.raises(asyncssh.SFTPFailure):
        await copy_tree(src, "/src", dst, "/dst/src", lambda e: None)
    assert "/dst/src/b.txt" not in dst.sftp.files


async def test_same_error_with_open_connection_is_per_entry():
    src, dst = make_endpoints()
    add_file(src.sftp, "/src/a.txt", b"aaa")
    add_file(src.sftp, "/src/b.txt", b"bbb")
    f = src.sftp.files["/src/a.txt"]
    f.fail_after = 0
    f.fail_exc = asyncssh.SFTPFailure("Failure")
    result = await copy_tree(src, "/src", dst, "/dst/src", lambda e: None)
    assert (result.copied, result.failed) == (1, 1)


# ---- a marked tree: everything inside was made by an earlier hopsync run -------


def marked_destination(source_host="hpc", source_path="/src"):
    src, dst = make_endpoints()
    add_file(src.sftp, "/src/sub/deep/f.txt", b"f")
    src.sftp.dir_meta.update({"/src": [0o750, 900], "/src/sub": [0o700, 800], "/src/sub/deep": [0o710, 700]})
    dst.sftp.dirs.update({"/dst/src", "/dst/src/sub"})  # left over from an interrupted run
    put_marker(dst.sftp, "/dst/src", host=source_host, path=source_path)
    return src, dst


async def test_directories_under_a_matching_marker_get_the_source_metadata():
    src, dst = marked_destination()
    await copy_tree(src, "/src", dst, "/dst/src", lambda e: None)
    assert dst.sftp.dir_meta["/dst/src"] == [0o750, 900]  # the top directory
    assert dst.sftp.dir_meta["/dst/src/sub"] == [0o700, 800]  # existed already, still ours
    assert dst.sftp.dir_meta["/dst/src/sub/deep"] == [0o710, 700]  # created in this run
    assert MARKER not in dst.sftp.files


async def test_a_marker_for_another_source_changes_nothing():
    src, dst = marked_destination(source_path="/somewhere-else")
    dst.sftp.dir_meta.update({"/dst/src": [0o755, 5], "/dst/src/sub": [0o755, 5]})
    await copy_tree(src, "/src", dst, "/dst/src", lambda e: None)
    assert dst.sftp.dir_meta["/dst/src"] == [0o755, 5]  # existing directories left as found
    assert dst.sftp.dir_meta["/dst/src/sub"] == [0o755, 5]
    assert dst.sftp.dir_meta["/dst/src/sub/deep"] == [0o710, 700]  # this one hopsync created


async def test_read_only_directory_in_a_marked_tree_still_ends_with_the_source_mode():
    src, dst = marked_destination()
    dst.sftp.dir_meta["/dst/src/sub"] = [0o555, 5]  # not writable: opened up, then set from the source
    await copy_tree(src, "/src", dst, "/dst/src", lambda e: None)
    assert dst.sftp.dir_meta["/dst/src/sub"] == [0o700, 800]
