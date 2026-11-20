"""Directory copies against the docker hosts."""

import asyncio

import pytest

from hopsync.cli import main
from hopsync.core.events import EntryFailed, EntrySkipped, TransferStarted, TreeProgress
from hopsync.core.transfer import copy_file, resolve_destination
from hopsync.core.tree import copy_tree
from test_integration import cli_copy, hpc_to_server

pytestmark = pytest.mark.integration

# Lists name, mode, mtime and type of everything under a directory.
DESCRIBE = "cd {d} && find . | sort | while read p; do stat -c '%n %a %Y %F' \"$p\"; done"
HASHES = "cd {d} && find . -type f | sort | while read p; do sha256sum \"$p\"; done"


async def run(conn, command):
    return (await conn.run(command, check=True)).stdout


async def build_tree(conn, name):
    """A nested tree with an empty dir, a >chunk file, modes and old mtimes."""
    await run(conn, f"""
        rm -rf {name} && mkdir -p {name}/empty {name}/sub/deep &&
        chmod g-s {name} {name}/empty {name}/sub {name}/sub/deep &&  # alpine dirs inherit setgid; we drop it on purpose
        head -c 6000000 /dev/urandom > {name}/big.bin &&
        head -c 1000 /dev/urandom > {name}/small.txt && chmod 640 {name}/small.txt &&
        printf '#!/bin/sh' > {name}/sub/run.sh && chmod 755 {name}/sub/run.sh &&
        head -c 100 /dev/urandom > {name}/sub/deep/x.dat &&
        touch -d '2020-01-02 03:04:05' {name}/big.bin {name}/small.txt {name}/sub/run.sh {name}/sub/deep/x.dat &&
        touch -d '2019-05-06 07:08:09' {name}/empty {name}/sub/deep &&
        touch -d '2018-01-01 00:00:00' {name}/sub {name}
    """)


async def copy_to(hosts, src_name, dst_name, **kwargs):
    """Copy hpc:src_name to server:dst_name (new top dir). Returns (result, events)."""
    events = []
    async with hpc_to_server(hosts) as (src, dst):
        result = await copy_tree(src, src_name, dst, dst_name, events.append, **kwargs)
    return result, events


async def test_nested_tree_contents_modes_and_times(hosts):
    async with hpc_to_server(hosts) as (src, dst):
        await build_tree(src.conn, "t1")
        result = await copy_tree(src, "t1", dst, "t1copy", lambda e: None, verify=True)
        assert (result.copied, result.skipped, result.failed) == (4, 0, 0)
        assert await run(dst.conn, DESCRIBE.format(d="t1copy")) == await run(src.conn, DESCRIBE.format(d="t1"))
        assert await run(dst.conn, HASHES.format(d="t1copy")) == await run(src.conn, HASHES.format(d="t1"))
        assert "./sub/run.sh 755 1577934245" in await run(dst.conn, DESCRIBE.format(d="t1copy"))


async def test_symlinks_skipped_by_default_and_copied_with_links(hosts):
    async with hpc_to_server(hosts) as (src, dst):
        await run(src.conn, "rm -rf t2 && mkdir t2 && echo hi > t2/file.txt && ln -s file.txt t2/link")
    result, events = await copy_to(hosts, "t2", "t2plain")
    assert [e.reason for e in events if isinstance(e, EntrySkipped)] == ["symlink"]
    assert result.copied == 1
    result, events = await copy_to(hosts, "t2", "t2links", links=True)
    assert result.copied == 2
    async with hpc_to_server(hosts) as (src, dst):
        assert not await exists_lstat(dst.conn, "t2plain/link")
        # The link must point at "file.txt" (argument order is right).
        assert (await run(dst.conn, "readlink t2links/link")).strip() == "file.txt"
        assert (await run(dst.conn, "cat t2links/link")).strip() == "hi"
    # Running again replaces the existing link instead of failing.
    result, _ = await copy_to(hosts, "t2", "t2links", links=True)
    assert result.failed == 0


async def exists_lstat(conn, path):
    return (await conn.run(f"test -L {path}", check=False)).exit_status == 0


async def test_fifo_is_skipped(hosts):
    async with hpc_to_server(hosts) as (src, dst):
        await run(src.conn, "rm -rf t3 && mkdir t3 && echo x > t3/ok && mkfifo t3/pipe")
    result, events = await copy_to(hosts, "t3", "t3copy")
    skipped = {e.path: e.reason for e in events if isinstance(e, EntrySkipped)}
    assert skipped == {"t3/pipe": "special file"}
    assert result.copied == 1


async def test_non_utf8_name_is_skipped_and_siblings_copied(hosts):
    async with hpc_to_server(hosts) as (src, dst):
        await run(src.conn, "rm -rf t4 && mkdir t4 && echo x > t4/ok && touch \"t4/$(printf 'bad\\377name')\"")
    result, events = await copy_to(hosts, "t4", "t4copy")
    assert [e.reason for e in events if isinstance(e, EntrySkipped)] == ["non-UTF-8 name"]
    assert (result.copied, result.skipped, result.failed) == (1, 1, 0)


async def test_unreadable_file_fails_exit_code_1(hosts, monkeypatch, capsys):
    async with hpc_to_server(hosts) as (src, dst):
        await run(src.conn, "rm -rf t5 && mkdir t5 && echo x > t5/ok && echo y > t5/secret && chmod 000 t5/secret")
    # Library level: one failure, the other file is still copied.
    result, events = await copy_to(hosts, "t5", "t5copy")
    assert (result.copied, result.failed) == (1, 1)
    assert any(isinstance(e, EntryFailed) and e.path == "t5/secret" for e in events)
    # Nothing was left behind on the destination for the unreadable file.
    async with hpc_to_server(hosts) as (src, dst):
        assert (await run(dst.conn, "ls t5copy")).split() == ["ok"]
    # CLI level: exit code 1 and the failure is listed.
    monkeypatch.setattr("getpass.getpass", lambda prompt="": hosts.password)
    argv = ["--config", str(hosts.config), "--password-for", "hpc", "copy", "hpc:t5", "server:t5cli"]
    code = await asyncio.to_thread(main, argv)
    assert code == 1
    assert "t5/secret" in capsys.readouterr().err


async def test_cli_copies_directory_and_single_file(hosts, monkeypatch, capsys):
    async with hpc_to_server(hosts) as (src, dst):
        await build_tree(src.conn, "t6")
    monkeypatch.setattr("getpass.getpass", lambda prompt="": hosts.password)
    base = ["--config", str(hosts.config), "--password-for", "hpc", "copy"]
    assert await asyncio.to_thread(main, [*base, "--verify", "hpc:t6", "server:t6cli"]) == 0
    assert "done 5.7M" in capsys.readouterr().out
    assert await asyncio.to_thread(main, [*base, "hpc:t6/small.txt", "server:t6cli/small2.txt"]) == 0


async def test_destination_rule_for_directories(hosts):
    async with hpc_to_server(hosts) as (src, dst):
        await run(src.conn, "rm -rf t7 && mkdir -p t7/a && echo x > t7/a/f")
        await run(dst.conn, "mkdir -p t7holder")
        # existing directory: copy goes INTO it, under the source name
        target = await resolve_destination(dst.sftp, "t7", "t7holder")
        assert target == "t7holder/t7"
        await copy_tree(src, "t7", dst, target, lambda e: None)
        assert (await run(dst.conn, "cat t7holder/t7/a/f")).strip() == "x"
        # new name: that name becomes the top directory
        target = await resolve_destination(dst.sftp, "t7", "t7newname")
        assert target == "t7newname"
        await copy_tree(src, "t7", dst, target, lambda e: None)
        assert (await run(dst.conn, "cat t7newname/a/f")).strip() == "x"


async def test_rerun_skips_everything_already_copied(hosts):
    async with hpc_to_server(hosts) as (src, dst):
        await build_tree(src.conn, "t8")
    first, _ = await copy_to(hosts, "t8", "t8copy")
    assert first.copied == 4
    second, events = await copy_to(hosts, "t8", "t8copy")
    assert (second.copied, second.skipped, second.bytes_copied) == (0, 4, 0)
    assert {e.reason for e in events if isinstance(e, EntrySkipped)} == {"already copied"}
    assert not any(isinstance(e, TransferStarted) for e in events)  # nothing was sent


async def test_interrupted_tree_resumes_on_rerun(hosts):
    async with hpc_to_server(hosts) as (src, dst):
        await run(src.conn, "rm -rf t9 && mkdir t9")
        for n in range(5):
            await run(src.conn, f"head -c 10000000 /dev/urandom > t9/f{n}.bin")

    async with hpc_to_server(hosts) as (src, dst):
        def kill_after_two_files(event):
            if isinstance(event, TreeProgress) and event.files_done == 2:
                dst.conn.abort()

        with pytest.raises(Exception):
            await copy_tree(src, "t9", dst, "t9copy", kill_after_two_files, chunk_size=1_000_000)

    async with hpc_to_server(hosts) as (src, dst):
        events = []
        result = await copy_tree(src, "t9", dst, "t9copy", events.append, verify=True)
        assert result.failed == 0
        already = [e for e in events if isinstance(e, EntrySkipped) and e.reason == "already copied"]
        assert len(already) >= 2
        assert result.copied + result.skipped == 5
        assert await run(dst.conn, HASHES.format(d="t9copy")) == await run(src.conn, HASHES.format(d="t9"))
        assert await run(dst.conn, "ls t9copy") == await run(src.conn, "ls t9")  # no .part left


async def test_single_file_copy_preserves_mtime_and_mode(hosts):
    async with hpc_to_server(hosts) as (src, dst):
        await run(src.conn, "echo hello > t10.txt && chmod 640 t10.txt && touch -d '2020-01-02 03:04:05' t10.txt")
        await copy_file(src, "t10.txt", dst, "t10.txt", lambda e: None)
        assert (await run(dst.conn, "stat -c '%a %Y' t10.txt")).strip() == "640 1577934245"


async def test_read_only_source_directory_and_rerun(hosts, monkeypatch):
    async with hpc_to_server(hosts) as (src, dst):
        await run(src.conn, """
            chmod -R u+w t11 2>/dev/null; rm -rf t11 && mkdir -p t11/ro &&
            echo one > t11/ro/a.txt && echo two > t11/ro/b.txt &&
            touch -d '2020-01-01 00:00:00' t11/ro/a.txt t11/ro/b.txt &&
            chmod g-s t11 t11/ro && chmod 555 t11/ro
        """)
        await run(dst.conn, "chmod -R u+w t11holder 2>/dev/null; rm -rf t11holder && mkdir t11holder")

        # First copy works even though the directory is read-only (children go
        # first), and the directories hopsync created get the source's modes.
        assert await cli_copy(hosts, monkeypatch, "hpc:t11", "server:t11holder") == 0
        assert (await run(dst.conn, "stat -c %a t11holder/t11/ro")).strip() == "555"

        # Change one source file (needs the directory writable for a moment).
        await run(src.conn, """
            chmod u+w t11/ro && echo changed-content > t11/ro/a.txt &&
            touch -d '2021-01-01 00:00:00' t11/ro/a.txt && chmod 555 t11/ro
        """)
        # Rerun without --overwrite: the changed file is refused, exit code 1.
        assert await cli_copy(hosts, monkeypatch, "hpc:t11", "server:t11holder") == 1
        assert (await run(dst.conn, "cat t11holder/t11/ro/a.txt")).strip() == "one"
        assert (await run(dst.conn, "stat -c %a t11holder/t11/ro")).strip() == "555"

        # With --overwrite it is replaced, and the directory mode ends correct.
        assert await cli_copy(hosts, monkeypatch, "--overwrite", "hpc:t11", "server:t11holder") == 0
        assert (await run(dst.conn, "cat t11holder/t11/ro/a.txt")).strip() == "changed-content"
        assert (await run(dst.conn, "stat -c %a t11holder/t11/ro")).strip() == "555"


STAT = "stat -c '%a %Y'"


async def build_existing_destination(conn):
    """server:t12holder/t12 already holds data hopsync must not disturb."""
    await run(conn, """
        chmod -R u+w t12holder 2>/dev/null; rm -rf t12holder &&
        mkdir -p t12holder/t12/keep t12holder/t12/grow &&
        echo "old different" > t12holder/t12/a.txt && echo unrelated > t12holder/t12/unrelated.txt &&
        echo k > t12holder/t12/keep/k.txt && touch -d '2020-01-01 00:00:00' t12holder/t12/keep/k.txt &&
        chmod g-s t12holder/t12 t12holder/t12/keep t12holder/t12/grow &&
        chmod 750 t12holder/t12/keep t12holder/t12/grow &&
        touch -d '2015-01-01 00:00:00' t12holder/t12/keep t12holder/t12/grow
    """)


async def build_source_for_existing(conn):
    await run(conn, """
        rm -rf t12 && mkdir -p t12/keep t12/grow &&
        echo "new content of a" > t12/a.txt && echo fresh > t12/new.txt &&
        echo k > t12/keep/k.txt && touch -d '2020-01-01 00:00:00' t12/keep/k.txt &&
        echo grown > t12/grow/g.txt &&
        chmod g-s t12 t12/keep t12/grow && chmod 700 t12/keep t12/grow &&
        touch -d '2010-01-01 00:00:00' t12/keep t12/grow t12
    """)


async def test_copy_into_existing_tree_never_modifies_existing_data(hosts, monkeypatch, capsys):
    async with hpc_to_server(hosts) as (src, dst):
        await build_source_for_existing(src.conn)
        await build_existing_destination(dst.conn)
        keep_before = await run(dst.conn, f"{STAT} t12holder/t12/keep")
        root_before = await run(dst.conn, f"{STAT} t12holder/t12")
        a_before = await run(dst.conn, "sha256sum t12holder/t12/a.txt")

        code = await cli_copy(hosts, monkeypatch, "hpc:t12", "server:t12holder")
        err = capsys.readouterr().err
        assert code == 1
        assert "t12/a.txt" in err and "exists and differs" in err

        assert await run(dst.conn, "sha256sum t12holder/t12/a.txt") == a_before  # (a) untouched
        assert (await run(dst.conn, "cat t12holder/t12/unrelated.txt")).strip() == "unrelated"  # (b)
        assert await run(dst.conn, f"{STAT} t12holder/t12/keep") == keep_before  # (c) mode and mtime
        assert (await run(dst.conn, f"{STAT} t12holder/t12/grow")).split()[0] == "750"  # (d) mode
        assert (await run(dst.conn, f"{STAT} t12holder/t12")).split()[0] == root_before.split()[0]
        assert (await run(dst.conn, "cat t12holder/t12/grow/g.txt")).strip() == "grown"
        assert (await run(dst.conn, "cat t12holder/t12/new.txt")).strip() == "fresh"

        # Same tree with --overwrite: (a) is replaced and directory metadata applied.
        code = await cli_copy(hosts, monkeypatch, "--overwrite", "hpc:t12", "server:t12holder")
        assert code == 0
        assert (await run(dst.conn, "cat t12holder/t12/a.txt")).strip() == "new content of a"
        assert (await run(dst.conn, f"{STAT} t12holder/t12/keep")).split()[0] == "700"
        assert (await run(dst.conn, "stat -c %Y t12holder/t12/keep")).strip() == (
            await run(src.conn, "stat -c %Y t12/keep")
        ).strip()


async def test_existing_read_only_destination_directory_ends_up_unchanged(hosts, monkeypatch):
    async with hpc_to_server(hosts) as (src, dst):
        await run(src.conn, "rm -rf t13 && mkdir -p t13/ro && echo added > t13/ro/new.txt && chmod g-s t13/ro")
        await run(dst.conn, """
            chmod -R u+w t13holder 2>/dev/null; rm -rf t13holder && mkdir -p t13holder/t13/ro &&
            chmod g-s t13holder/t13/ro && chmod 555 t13holder/t13/ro
        """)
        code = await cli_copy(hosts, monkeypatch, "hpc:t13", "server:t13holder")
        assert code == 0
        assert (await run(dst.conn, "cat t13holder/t13/ro/new.txt")).strip() == "added"
        assert (await run(dst.conn, "stat -c %a t13holder/t13/ro")).strip() == "555"  # as found


MARKER_FILE = ".hopsync-incomplete.json"


async def exists_on(conn, path):
    return (await conn.run(f"test -e {path}", check=False)).exit_status == 0


async def test_rerun_of_interrupted_tree_to_new_name_does_not_nest(hosts, monkeypatch):
    from hopsync.cli import build_parser, run_copy

    async with hpc_to_server(hosts) as (src, dst):
        await run(src.conn, "rm -rf t15 && mkdir t15")
        for n in range(5):
            await run(src.conn, f"head -c 10000000 /dev/urandom > t15/f{n}.bin")
        await run(dst.conn, "rm -rf t15new")

        monkeypatch.setattr("getpass.getpass", lambda prompt="": hosts.password)
        argv = ["--config", str(hosts.config), "--password-for", "hpc", "copy", "hpc:t15", "server:t15new"]
        task = asyncio.create_task(run_copy(build_parser().parse_args(argv)))
        for _ in range(500):  # interrupt once the second file has started
            await asyncio.sleep(0.01)
            if await exists_on(dst.conn, "t15new/f1.bin.hopsync-part"):
                break
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert await exists_on(dst.conn, f"t15new/{MARKER_FILE}")  # unfinished: marker present

        # The SAME command again: continues in t15new instead of making t15new/t15.
        assert await cli_copy(hosts, monkeypatch, "hpc:t15", "server:t15new") == 0
        assert not await exists_on(dst.conn, "t15new/t15")
        assert not await exists_on(dst.conn, f"t15new/{MARKER_FILE}")
        assert await run(dst.conn, HASHES.format(d="t15new")) == await run(src.conn, HASHES.format(d="t15"))


async def test_completed_tree_copy_leaves_no_marker_and_existing_dir_gets_none(hosts):
    async with hpc_to_server(hosts) as (src, dst):
        await run(src.conn, "rm -rf t16 && mkdir -p t16/sub && echo x > t16/sub/f")
        await run(dst.conn, "rm -rf t16new t16holder && mkdir t16holder")
        await copy_tree(src, "t16", dst, "t16new", lambda e: None)
        assert not await exists_on(dst.conn, f"t16new/{MARKER_FILE}")

        # Copying into an existing directory: the directory itself never gets a marker.
        target = await resolve_destination(dst.sftp, "t16", "t16holder", src_host="hpc", src_is_dir=True)
        assert target == "t16holder/t16"
        seen = []
        await copy_tree(src, "t16", dst, target, lambda e: seen.append(1))
        assert not await exists_on(dst.conn, f"t16holder/{MARKER_FILE}")
        assert not await exists_on(dst.conn, f"t16holder/t16/{MARKER_FILE}")


async def test_marker_for_a_different_source_means_normal_nesting(hosts):
    async with hpc_to_server(hosts) as (src, dst):
        await run(src.conn, "rm -rf t17 && mkdir t17 && echo x > t17/f")
        await run(dst.conn, f"""rm -rf t17dst && mkdir t17dst &&
            echo '{{"version": 1, "source_host": "hpc", "source_path": "some-other-dir"}}' > t17dst/{MARKER_FILE}""")
        target = await resolve_destination(dst.sftp, "t17", "t17dst", src_host="hpc", src_is_dir=True)
        assert target == "t17dst/t17"
        await copy_tree(src, "t17", dst, target, lambda e: None)
        assert (await run(dst.conn, "cat t17dst/t17/f")).strip() == "x"
        assert await exists_on(dst.conn, f"t17dst/{MARKER_FILE}")  # someone else's marker untouched


DIR_LISTING = "cd {d} && find . -type d | sort | while read p; do stat -c '%n %a %Y' \"$p\"; done"


async def test_rerun_of_an_interrupted_cli_copy_gives_directories_the_source_metadata(hosts, monkeypatch):
    from hopsync.cli import build_parser, run_copy

    async with hpc_to_server(hosts) as (src, dst):
        await run(src.conn, "rm -rf t19 && mkdir -p t19/sub/deep")
        for n in range(4):
            await run(src.conn, f"head -c 10000000 /dev/urandom > t19/f{n}.bin")
        await run(src.conn, """
            echo one > t19/sub/a.txt && echo two > t19/sub/deep/b.txt &&
            chmod g-s t19 t19/sub t19/sub/deep && chmod 750 t19/sub && chmod 700 t19/sub/deep &&
            touch -d '2017-03-04 05:06:07' t19/sub/deep && touch -d '2016-02-03 04:05:06' t19/sub &&
            touch -d '2015-01-02 03:04:05' t19
        """)
        await run(dst.conn, "rm -rf t19new")

        monkeypatch.setattr("getpass.getpass", lambda prompt="": hosts.password)
        argv = ["--config", str(hosts.config), "--password-for", "hpc", "copy", "hpc:t19", "server:t19new"]
        task = asyncio.create_task(run_copy(build_parser().parse_args(argv)))
        for _ in range(500):  # interrupt during the second big file, once the folders exist
            await asyncio.sleep(0.01)
            if await exists_on(dst.conn, "t19new/f1.bin.hopsync-part") and await exists_on(dst.conn, "t19new/sub/deep"):
                break
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert await exists_on(dst.conn, f"t19new/{MARKER_FILE}")

        # The SAME command again, without --overwrite.
        assert await cli_copy(hosts, monkeypatch, "hpc:t19", "server:t19new") == 0
        assert not await exists_on(dst.conn, f"t19new/{MARKER_FILE}")
        assert not await exists_on(dst.conn, "t19new/t19")
        assert await run(dst.conn, DIR_LISTING.format(d="t19new")) == await run(src.conn, DIR_LISTING.format(d="t19"))
