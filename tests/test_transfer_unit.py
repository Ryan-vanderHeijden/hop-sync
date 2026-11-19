"""Pipeline tests with in-memory fakes (no network)."""

import asyncio
import hashlib
import stat
from types import SimpleNamespace

import asyncssh
import pytest

from hopsync.core import resume
from hopsync.core.errors import VerificationFailed
from hopsync.core.events import TransferFinished, TransferProgress, TransferStarted, Verifying
from hopsync.core.session import Endpoint
from hopsync.core.transfer import TransferError, copy_file, resolve_destination


class FakeFile:
    def __init__(self, data=b"", fail_after=None):
        self.data = bytearray(data)
        self.pos = 0
        self.fail_after = fail_after  # raise after this many calls
        self.calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def _maybe_fail(self):
        self.calls += 1
        if self.fail_after is not None and self.calls > self.fail_after:
            raise RuntimeError("boom")

    async def seek(self, offset):
        self.pos = offset

    async def read(self, n=-1):
        self._maybe_fail()
        await asyncio.sleep(0)
        end = len(self.data) if n < 0 else self.pos + n
        chunk = bytes(self.data[self.pos : end])
        self.pos += len(chunk)
        return chunk

    async def write(self, chunk, offset):
        self._maybe_fail()
        await asyncio.sleep(0)
        assert offset <= len(self.data)  # never skips ahead
        self.data[offset : offset + len(chunk)] = chunk  # overwrites like a real file


class FakeSFTP:
    """files: path -> FakeFile; dirs: set of directory paths."""

    def __init__(self, files=None, dirs=(), size_lie=0):
        self.files = files or {}
        self.dirs = set(dirs) | {"."}
        self.size_lie = size_lie

    def open(self, path, mode):
        if mode == "wb":  # truncate (keep the object so fail_after survives)
            existing = self.files.setdefault(path, FakeFile())
            existing.data = bytearray()
        if path not in self.files:
            raise asyncssh.SFTPNoSuchFile("no such file")
        return self.files[path]

    async def stat(self, path):
        if path in self.dirs:
            return SimpleNamespace(permissions=stat.S_IFDIR | 0o755, size=0, mtime=0)
        if path in self.files:
            size = len(self.files[path].data) + (self.size_lie if path.endswith(".hopsync-part") else 0)
            return SimpleNamespace(permissions=stat.S_IFREG | 0o644, size=size, mtime=100)
        raise asyncssh.SFTPNoSuchFile("no such file")

    async def posix_rename(self, old, new):
        self.files[new] = self.files.pop(old)

    async def remove(self, path):
        if path not in self.files:
            raise asyncssh.SFTPNoSuchFile("no such file")
        del self.files[path]


class FakeConn:
    """Pretends to run sha256sum over the fake files of one SFTP."""

    def __init__(self, sftp, lie=False):
        self.sftp, self.lie = sftp, lie

    async def run(self, command, check=False):
        path = command.split("-- ", 1)[1].strip("'")
        digest = hashlib.sha256(bytes(self.sftp.files[path].data)).hexdigest()
        if self.lie:
            digest = "0" * 64
        return SimpleNamespace(exit_status=0, stdout=f"{digest}  {path}\n", stderr="")


def endpoints(src_files, dst_files=None, *, lie=False, **dst_kwargs):
    src_sftp = FakeSFTP(src_files)
    dst_sftp = FakeSFTP(dst_files, **dst_kwargs)
    src = Endpoint("hpc", FakeConn(src_sftp), src_sftp)
    dst = Endpoint("server", FakeConn(dst_sftp, lie), dst_sftp)
    return src, dst


PART = "b.hopsync-part"
META = "b.hopsync-part.json"


async def test_copy_chunks_events_and_order():
    data = bytes(range(256)) * 10  # 2560 bytes
    src, dst = endpoints({"a": FakeFile(data)})
    events = []
    n = await copy_file(src, "a", dst, "b", events.append, chunk_size=1000, queue_depth=2)
    assert n == len(data)
    assert bytes(dst.sftp.files["b"].data) == data
    # Only the final file remains: part and sidecar are gone.
    assert PART not in dst.sftp.files and META not in dst.sftp.files
    assert isinstance(events[0], TransferStarted) and events[0].total_bytes == 2560
    assert events[0].resumed_from == 0
    progress = [e.bytes_done for e in events if isinstance(e, TransferProgress)]
    assert progress == [1000, 2000, 2560]
    assert isinstance(events[-1], TransferFinished) and events[-1].bytes_done == 2560


async def test_reader_failure_propagates_and_keeps_part():
    src, dst = endpoints({"a": FakeFile(b"x" * 5000, fail_after=2)})
    with pytest.raises(RuntimeError, match="boom"):
        await copy_file(src, "a", dst, "b", lambda e: None, chunk_size=1000)
    assert PART in dst.sftp.files and META in dst.sftp.files and "b" not in dst.sftp.files


async def test_writer_failure_cancels_reader():
    src_file = FakeFile(b"x" * 100_000)
    src, dst = endpoints({"a": src_file}, {PART: FakeFile(fail_after=1)})
    with pytest.raises(RuntimeError, match="boom"):
        await copy_file(src, "a", dst, "b", lambda e: None, chunk_size=10, queue_depth=2)
    # The reader must have stopped long before reading the whole file.
    assert src_file.pos < 100_000


async def test_size_mismatch_removes_part():
    src, dst = endpoints({"a": FakeFile(b"abc")}, size_lie=1)
    with pytest.raises(VerificationFailed, match="size mismatch"):
        await copy_file(src, "a", dst, "b", lambda e: None)
    assert PART not in dst.sftp.files and META not in dst.sftp.files


async def test_directory_source_refused():
    src, dst = endpoints({})
    src.sftp.dirs.add("d")
    with pytest.raises(TransferError, match="milestone 2b"):
        await copy_file(src, "d", dst, "b", lambda e: None)


async def test_resume_reports_offset_and_completes():
    data = bytes(range(256)) * 10
    src, dst = endpoints({"a": FakeFile(data)})
    # First attempt dies after the first chunk is written.
    dst.sftp.files[PART] = FakeFile(fail_after=1)
    with pytest.raises(RuntimeError):
        await copy_file(src, "a", dst, "b", lambda e: None, chunk_size=1000)
    dst.sftp.files[PART].fail_after = None
    done_so_far = len(dst.sftp.files[PART].data)
    assert 0 < done_so_far < len(data)

    events = []
    await copy_file(src, "a", dst, "b", events.append, chunk_size=1000)
    assert events[0].resumed_from == max(0, done_so_far - 1000)  # rewound one chunk
    first = next(e for e in events if isinstance(e, TransferProgress))
    assert first.bytes_done > events[0].resumed_from  # counts from the offset
    assert bytes(dst.sftp.files["b"].data) == data


async def test_resume_repairs_hole_in_last_chunk():
    data = bytes(range(1, 251)) * 12  # 3000 bytes, no zero bytes
    src, dst = endpoints({"a": FakeFile(data)})
    # A part of 2500 bytes whose last chunk (bytes 1500-2500) has a hole.
    damaged = bytearray(data[:2500])
    damaged[2000:2200] = bytes(200)
    dst.sftp.files[PART] = FakeFile(bytes(damaged))
    await resume.write_meta(dst.sftp, "b", resume.make_meta("hpc", "a", 3000, 100))
    events = []
    await copy_file(src, "a", dst, "b", events.append, chunk_size=1000)  # no verify
    assert events[0].resumed_from == 1500
    assert bytes(dst.sftp.files["b"].data) == data


async def test_changed_source_starts_over():
    src, dst = endpoints({"a": FakeFile(b"y" * 3000)})
    dst.sftp.files[PART] = FakeFile(fail_after=1)
    with pytest.raises(RuntimeError):
        await copy_file(src, "a", dst, "b", lambda e: None, chunk_size=1000)
    dst.sftp.files[PART].fail_after = None
    src.sftp.files["a"] = FakeFile(b"z" * 2000)  # different size
    events = []
    await copy_file(src, "a", dst, "b", events.append, chunk_size=1000)
    assert events[0].resumed_from == 0
    assert bytes(dst.sftp.files["b"].data) == b"z" * 2000


async def test_verify_ok_streams_hash():
    src, dst = endpoints({"a": FakeFile(b"hello world")})
    events = []
    await copy_file(src, "a", dst, "b", events.append, verify=True)
    assert [e.method for e in events if isinstance(e, Verifying)] == ["sha256"]


async def test_failed_verification_deletes_part_and_sidecar():
    src, dst = endpoints({"a": FakeFile(b"hello world")}, lie=True)
    with pytest.raises(VerificationFailed, match="checksum mismatch"):
        await copy_file(src, "a", dst, "b", lambda e: None, verify=True)
    assert set(dst.sftp.files) == set()


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
