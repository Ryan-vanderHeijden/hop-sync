"""In-memory stand-ins for SFTP files and servers (used by unit tests)."""

import asyncio
import hashlib
import posixpath
import stat
from types import SimpleNamespace

import asyncssh

from hopsync.core.session import Endpoint


class FakeFile:
    def __init__(self, data=b"", fail_after=None, mode=0o644, mtime=100):
        self.mode = mode
        self.fail_exc = None  # exception to raise instead of RuntimeError
        self.mtime = mtime
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
            raise self.fail_exc or RuntimeError("boom")

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
        self.dir_meta = {}  # dir path -> [mode, mtime]
        self.links = {}  # link path -> target
        self.special = {}  # path -> mode (fifo etc.)
        self.raw_names = {}  # dir path -> [undecodable names as bytes]
        self.setstat_log = []  # paths in the order setstat was called
        self.setstat_errors = {}  # path -> exception to raise from setstat

    def open(self, path, mode):
        if mode == "wb":  # truncate (keep the object so fail_after survives)
            existing = self.files.setdefault(path, FakeFile())
            existing.data = bytearray()
        if path not in self.files:
            raise asyncssh.SFTPNoSuchFile("no such file")
        return self.files[path]

    async def stat(self, path):
        if path in self.dirs:
            mode, mtime = self.dir_meta.get(path, [0o755, 0])
            return SimpleNamespace(permissions=stat.S_IFDIR | mode, size=0, mtime=mtime)
        if path in self.files:
            f = self.files[path]
            size = len(f.data) + (self.size_lie if path.endswith(".hopsync-part") else 0)
            return SimpleNamespace(permissions=stat.S_IFREG | f.mode, size=size, mtime=f.mtime)
        raise asyncssh.SFTPNoSuchFile("no such file")

    async def lstat(self, path):
        if path in self.links:
            return SimpleNamespace(permissions=stat.S_IFLNK | 0o777, size=0, mtime=0)
        return await self.stat(path)

    async def setstat(self, path, attrs):
        self.setstat_log.append(path)
        if path in self.setstat_errors:
            raise self.setstat_errors[path]
        if path in self.dirs:
            self.dir_meta[path] = [attrs.permissions, attrs.mtime]
        else:
            self.files[path].mode = attrs.permissions
            self.files[path].mtime = attrs.mtime

    async def chmod(self, path, mode):
        old = self.dir_meta.get(path, [0o755, 0])
        self.dir_meta[path] = [mode, old[1]]

    async def mkdir(self, path):
        assert posixpath.dirname(path) in self.dirs, "parent must exist"
        self.dirs.add(path)

    async def readlink(self, path):
        return self.links[path]

    async def symlink(self, target, path):
        self.links[path] = target

    async def readdir(self, path):
        path = path.decode()
        entries = []

        def add(name, mode, size=0, mtime=0):
            attrs = SimpleNamespace(permissions=mode, size=size, mtime=mtime)
            entries.append(SimpleNamespace(filename=name, attrs=attrs))

        for p in self.dirs:
            if p != "." and posixpath.dirname(p) == path:
                mode, mtime = self.dir_meta.get(p, [0o755, 0])
                add(posixpath.basename(p).encode(), stat.S_IFDIR | mode, mtime=mtime)
        for p, f in self.files.items():
            if posixpath.dirname(p) == path:
                add(posixpath.basename(p).encode(), stat.S_IFREG | f.mode, len(f.data), f.mtime)
        for p, target in self.links.items():
            if posixpath.dirname(p) == path:
                add(posixpath.basename(p).encode(), stat.S_IFLNK | 0o777)
        for p, mode in self.special.items():
            if posixpath.dirname(p) == path:
                add(posixpath.basename(p).encode(), mode)
        for raw in self.raw_names.get(path, []):
            add(raw, stat.S_IFREG | 0o644)
        add(b".", stat.S_IFDIR | 0o755)
        add(b"..", stat.S_IFDIR | 0o755)
        return entries

    async def rename(self, old, new):
        # Like OpenSSH's plain rename: refuses to replace an existing target.
        if new in self.files:
            raise asyncssh.SFTPFailure("Failure")
        self.files[new] = self.files.pop(old)

    async def posix_rename(self, old, new):
        self.files[new] = self.files.pop(old)

    async def remove(self, path):
        if path in self.links:
            del self.links[path]
            return
        if path not in self.files:
            raise asyncssh.SFTPNoSuchFile("no such file")
        del self.files[path]


class FakeConn:
    """Pretends to run sha256sum over the fake files of one SFTP."""

    def __init__(self, sftp, lie=False):
        self.sftp, self.lie = sftp, lie
        self.closed = False

    def is_closed(self):
        return self.closed

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




def add_file(sftp, path, data=b"data", mode=0o644, mtime=100):
    """Create a file and any missing parent directories."""
    parent = posixpath.dirname(path)
    while parent not in sftp.dirs:
        sftp.dirs.add(parent)
        parent = posixpath.dirname(parent)
    sftp.files[path] = FakeFile(data, mode=mode, mtime=mtime)
