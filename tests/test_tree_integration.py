"""Directory copies against the docker hosts."""

import asyncio

import pytest

from hopsync.cli import main
from hopsync.core.events import EntryFailed, EntrySkipped, TransferStarted, TreeProgress
from hopsync.core.transfer import copy_file, resolve_destination
from hopsync.core.tree import copy_tree
from test_integration import hpc_to_server

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
    assert "4 copied, 0 skipped, 0 failed" in capsys.readouterr().out
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


async def test_read_only_source_directory_and_rerun(hosts):
    async with hpc_to_server(hosts) as (src, dst):
        await run(src.conn, """
            chmod -R u+w t11 2>/dev/null; rm -rf t11 && mkdir -p t11/ro &&
            echo one > t11/ro/a.txt && echo two > t11/ro/b.txt &&
            touch -d '2020-01-01 00:00:00' t11/ro/a.txt t11/ro/b.txt &&
            chmod g-s t11 t11/ro && chmod 555 t11/ro
        """)
    # First copy works even though the directory is read-only (children go first).
    first, _ = await copy_to(hosts, "t11", "t11copy")
    assert (first.copied, first.failed) == (2, 0)
    async with hpc_to_server(hosts) as (src, dst):
        assert (await run(dst.conn, "stat -c %a t11copy/ro")).strip() == "555"
        # Change one source file (needs the directory writable for a moment).
        await run(src.conn, """
            chmod u+w t11/ro && echo changed-content > t11/ro/a.txt &&
            touch -d '2021-01-01 00:00:00' t11/ro/a.txt && chmod 555 t11/ro
        """)
    second, events = await copy_to(hosts, "t11", "t11copy")
    assert (second.copied, second.skipped, second.failed) == (1, 1, 0)
    assert [e.path for e in events if isinstance(e, EntrySkipped)] == ["t11/ro/b.txt"]
    async with hpc_to_server(hosts) as (src, dst):
        assert (await run(dst.conn, "cat t11copy/ro/a.txt")).strip() == "changed-content"
        assert (await run(dst.conn, "stat -c %a t11copy/ro")).strip() == "555"
