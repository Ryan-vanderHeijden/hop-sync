"""Pipeline tests with in-memory fakes (no network)."""

import asyncio
import stat
from types import SimpleNamespace

import asyncssh
import pytest

from hopsync.core.events import TransferFinished, TransferProgress, TransferStarted
from hopsync.core.transfer import TransferError, copy_file, resolve_destination


class FakeFile:
    def __init__(self, data=b"", fail_after=None):
        self.data = bytearray(data)
        self.pos = 0
        self.fail_after = fail_after  # raise after this many calls
        self.calls = 0
        self.cancelled = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def _maybe_fail(self):
        self.calls += 1
        if self.fail_after is not None and self.calls > self.fail_after:
            raise RuntimeError("boom")

    async def read(self, n):
        self._maybe_fail()
        await asyncio.sleep(0)
        chunk = bytes(self.data[self.pos : self.pos + n])
        self.pos += len(chunk)
        return chunk

    async def write(self, chunk, offset):
        self._maybe_fail()
        try:
            await asyncio.sleep(0)
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        assert offset == len(self.data)  # chunks arrive in order
        self.data += chunk


class FakeSFTP:
    """files: path -> FakeFile; dirs: set of directory paths."""

    def __init__(self, files=None, dirs=(), size_lie=0):
        self.files = files or {}
        self.dirs = set(dirs) | {"."}
        self.size_lie = size_lie

    def open(self, path, mode):
        if mode == "wb":
            self.files[path] = self.files.get(path) or FakeFile()
        return self.files[path]

    async def stat(self, path):
        if path in self.dirs:
            return SimpleNamespace(permissions=stat.S_IFDIR | 0o755, size=0)
        if path in self.files:
            size = len(self.files[path].data) + self.size_lie
            return SimpleNamespace(permissions=stat.S_IFREG | 0o644, size=size)
        raise asyncssh.SFTPNoSuchFile("no such file")


async def test_copy_chunks_events_and_order():
    data = bytes(range(256)) * 10  # 2560 bytes
    src = FakeSFTP({"a": FakeFile(data)})
    dst = FakeSFTP()
    events = []
    n = await copy_file(src, "a", dst, "b", events.append, chunk_size=1000, queue_depth=2)
    assert n == len(data)
    assert bytes(dst.files["b"].data) == data
    assert isinstance(events[0], TransferStarted) and events[0].total_bytes == 2560
    progress = [e.bytes_done for e in events if isinstance(e, TransferProgress)]
    assert progress == [1000, 2000, 2560]
    assert isinstance(events[-1], TransferFinished) and events[-1].bytes_done == 2560


async def test_reader_failure_propagates():
    src = FakeSFTP({"a": FakeFile(b"x" * 5000, fail_after=2)})
    with pytest.raises(RuntimeError, match="boom"):
        await copy_file(src, "a", FakeSFTP(), "b", lambda e: None, chunk_size=1000)


async def test_writer_failure_cancels_reader():
    src_file = FakeFile(b"x" * 100_000)
    dst = FakeSFTP({"b": FakeFile(fail_after=1)})
    with pytest.raises(RuntimeError, match="boom"):
        await copy_file(FakeSFTP({"a": src_file}), "a", dst, "b", lambda e: None, chunk_size=10, queue_depth=2)
    # The reader must have stopped long before reading the whole file.
    assert src_file.pos < 100_000


async def test_size_mismatch():
    src = FakeSFTP({"a": FakeFile(b"abc")})
    with pytest.raises(TransferError, match="size mismatch"):
        await copy_file(src, "a", FakeSFTP(size_lie=1), "b", lambda e: None)


async def test_directory_source_refused():
    with pytest.raises(TransferError, match="milestone 2b"):
        await copy_file(FakeSFTP(dirs={"d"}), "d", FakeSFTP(), "b", lambda e: None)


async def test_resolve_into_existing_dir():
    dst = FakeSFTP(dirs={"out"})
    assert await resolve_destination(dst, "/x/y/file.bin", "out") == "out/file.bin"


async def test_resolve_new_name():
    dst = FakeSFTP(dirs={"out"})
    assert await resolve_destination(dst, "/x/file.bin", "out/new.bin") == "out/new.bin"
    assert await resolve_destination(dst, "/x/file.bin", "new.bin") == "new.bin"


async def test_resolve_missing_parent():
    with pytest.raises(TransferError, match="does not exist"):
        await resolve_destination(FakeSFTP(), "/x/f", "nodir/new.bin")
