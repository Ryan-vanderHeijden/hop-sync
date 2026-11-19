"""Command-line interface: a thin layer over hopsync.core."""

import argparse
import asyncio
import getpass
import collections
import stat
import sys
import time
from datetime import datetime

import asyncssh

from .core.errors import TransferError
from .core.jobs import JobChanged, JobEntryEvent, JobManager, JobState
from .core.events import EntryFailed, EntrySkipped
from .core.listing import list_dir
from .core.paths import RemotePath, parse_remote
from .core.session import ConnectError, KeyNeedsPassphrase, connect
from .core.sessions import SessionPool


def human_size(num: float) -> str:
    """Format a byte count like 1.5M."""
    for unit in ("B", "K", "M", "G", "T"):
        if num < 1024 or unit == "T":
            return f"{num:.0f}{unit}" if unit == "B" else f"{num:.1f}{unit}"
        num /= 1024


def format_eta(seconds: float) -> str:
    minutes, secs = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours}:{minutes:02d}:{secs:02d}" if hours else f"{minutes}:{secs:02d}"


def format_duration(seconds: float) -> str:
    """Short duration like 42s, 3m05s, 1h02m."""
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    minutes, secs = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m{secs:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m"


def show(path: RemotePath) -> str:
    return f"{path.host}:{path.path}"


class Reporter:
    """Turns job events into text: one line per change plus one progress line."""

    def __init__(self) -> None:
        self.jobs: dict = {}  # latest snapshot per job id
        self.entry_counts: dict = collections.defaultdict(lambda: {"skipped": 0, "failed": 0})
        self.last_draw = 0.0
        self.window = collections.deque()  # (time, total bytes done), for the speed

    def __call__(self, event) -> None:
        if isinstance(event, JobChanged):
            self.job_changed(event.job)
        elif isinstance(event, JobEntryEvent):
            self.entry_event(event)

    def job_changed(self, job) -> None:
        old = self.jobs.get(job.id)
        self.jobs[job.id] = job
        if old is None or old.state is not job.state or old.attempt != job.attempt:
            self.state_line(job)
        elif job.message.startswith("reconnecting") and old.message != job.message:
            self.line(f"[{job.id}] {job.message}", sys.stderr)
        self.draw()

    def state_line(self, job) -> None:
        tag = f"[{job.id}]"
        if job.state is JobState.RUNNING:
            self.line(f"{tag} running {show(job.src)} -> {show(job.dst)}")
        elif job.state is JobState.DONE:
            took = format_duration((job.finished_at or 0) - (job.started_at or 0))
            counts = self.entry_counts[job.id]
            extra = f" ({counts['skipped']} skipped)" if counts["skipped"] else ""
            self.line(f"{tag} done {human_size(job.bytes_total)} in {took}{extra}")
        elif job.state is JobState.FAILED:
            self.line(f"{tag} failed: {job.message}", sys.stderr)
        elif job.state is JobState.CANCELLED:
            self.line(f"{tag} cancelled")

    def entry_event(self, wrapped) -> None:
        tag, event = f"[{wrapped.job_id}]", wrapped.event
        if isinstance(event, EntrySkipped):
            self.entry_counts[wrapped.job_id]["skipped"] += 1
            self.line(f"{tag} skipped {event.path} ({event.reason})")
        elif isinstance(event, EntryFailed):
            self.entry_counts[wrapped.job_id]["failed"] += 1
            self.line(f"{tag} FAILED  {event.path}: {event.message}", sys.stderr)

    def line(self, text: str, stream=None) -> None:
        """Print a line without leaving a half-drawn progress line behind."""
        self.clear()
        print(text, file=stream or sys.stdout, flush=True)
        self.last_draw = 0.0  # redraw the progress line soon

    def clear(self) -> None:
        if sys.stderr.isatty():
            sys.stderr.write("\r" + " " * 78 + "\r")

    def draw(self) -> None:
        """One combined progress line across all running jobs."""
        now = time.monotonic()
        running = [j for j in self.jobs.values() if j.state is JobState.RUNNING]
        if not running or now - self.last_draw < 0.1:
            return
        self.last_draw = now
        queued = sum(1 for j in self.jobs.values() if j.state is JobState.QUEUED)
        done = sum(j.bytes_done for j in running)
        found = sum(j.bytes_total for j in running)
        # Speed counts only bytes really sent in this run (all jobs, so the
        # total never goes down when a job ends).
        rate = self.rate(now, sum(j.bytes_transferred for j in self.jobs.values()))
        percent = 100 * done / found if found else 0
        line = (
            f"{percent:5.1f}%  {human_size(done)}/{human_size(found)}  {rate / 1e6:.1f} MB/s  "
            f"{len(running)} running, {queued} queued"
        )
        sys.stderr.write("\r" + line.ljust(78))
        sys.stderr.flush()

    def rate(self, now: float, done: int) -> float:
        # Speed over the last ~5 seconds.
        self.window.append((now, done))
        while now - self.window[0][0] > 5:
            self.window.popleft()
        then, before = self.window[0]
        return max(0, done - before) / (now - then) if now > then else 0.0


class PassphraseHandler:
    """Asks for a key passphrase once per host, then retries the jobs that needed it."""

    def __init__(self, pool: SessionPool, manager_getter, passwords: dict) -> None:
        self.pool, self.manager_getter, self.passwords = pool, manager_getter, passwords
        self.asked: set = set()
        self.tasks: list = []

    def __call__(self, event) -> None:
        if not isinstance(event, JobChanged):
            return
        job = event.job
        if job.state is JobState.FAILED and job.error_kind == "passphrase" and job.error_alias not in self.asked:
            self.asked.add(job.error_alias)  # a wrong passphrase is not asked for again
            self.tasks.append(asyncio.get_running_loop().create_task(self.ask(job.error_alias)))

    async def ask(self, alias: str) -> None:
        # getpass blocks, so it runs in a thread: other jobs keep moving.
        passphrase = await asyncio.to_thread(getpass.getpass, f"Passphrase for key of {alias}: ")
        self.pool.set_credentials(alias, password=self.passwords.get(alias), passphrase=passphrase)
        manager = self.manager_getter()
        for job in manager.list():
            if job.state is JobState.FAILED and job.error_kind == "passphrase" and job.error_alias == alias:
                manager.retry(job.id)


async def open_connection(alias: str, args, passwords: dict):
    """Connect (for `list`), asking for a key passphrase once if the key needs one."""
    password = passwords.get(alias)
    try:
        return await connect(alias, password=password, config_path=args.config)
    except KeyNeedsPassphrase:
        passphrase = getpass.getpass(f"Passphrase for key of {alias}: ")
        return await connect(
            alias, password=password, passphrase=passphrase, config_path=args.config
        )


def ask_passwords(args) -> dict:
    """Prompt once for each host listed with --password-for."""
    return {a: getpass.getpass(f"Password for {a}: ") for a in args.password_for}


async def run_list(args) -> None:
    remote = parse_remote(args.target)
    passwords = ask_passwords(args)
    conn = await open_connection(remote.host, args, passwords)
    try:
        async with conn.start_sftp_client() as sftp:
            for e in await list_dir(sftp, remote.path):
                kind = "d" if e.is_dir else "l" if e.is_link else "-"
                when = datetime.fromtimestamp(e.mtime).strftime("%Y-%m-%d %H:%M")
                print(f"{kind} {human_size(e.size):>8} {when}  {e.name}")
    finally:
        conn.close()
        await conn.wait_closed()


async def require_directory(pool: SessionPool, dst: RemotePath) -> None:
    """With several sources the destination must be an existing directory."""
    endpoint = await pool.get(dst.host)
    try:
        attrs = await endpoint.sftp.stat(dst.path)
        is_dir = stat.S_ISDIR(attrs.permissions or 0)
    except asyncssh.SFTPNoSuchFile:
        is_dir = False
    if not is_dir:
        raise TransferError("with several sources, the destination must be an existing directory")


async def wait_for_jobs(manager: JobManager, passphrases: PassphraseHandler) -> None:
    """Wait until all jobs settle, including retries after a passphrase prompt."""
    while True:
        await manager.wait_idle()
        if not passphrases.tasks:
            return
        await asyncio.gather(*passphrases.tasks)
        passphrases.tasks.clear()


async def run_copy(args) -> int:
    """Copy one or more files/directories. Returns the exit code."""
    *source_specs, dest_spec = args.paths
    sources = [parse_remote(spec) for spec in source_specs]
    dst = parse_remote(dest_spec)
    if any(source.host == dst.host for source in sources):
        raise TransferError("source and destination are on the same host; copy on the host itself")

    passwords = ask_passwords(args)
    pool = SessionPool(config_path=args.config)
    for alias, password in passwords.items():
        pool.set_credentials(alias, password=password)
    reporter = Reporter()
    manager = None
    passphrases = PassphraseHandler(pool, lambda: manager, passwords)

    def on_event(event) -> None:
        reporter(event)
        passphrases(event)

    manager = JobManager(pool, on_event, args.jobs)
    try:
        if len(sources) > 1:
            await require_directory(pool, dst)
        for source in sources:
            manager.add(source, dst, verify=args.verify, links=args.links, overwrite=args.overwrite)
        await wait_for_jobs(manager, passphrases)
    except asyncio.CancelledError:  # Ctrl+C
        manager.cancel_all()
        await manager.wait_idle()
        reporter.clear()
        raise
    finally:
        await pool.close_all()
    reporter.clear()
    return 1 if any(job.state is not JobState.DONE for job in manager.list()) else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="hopsync")
    parser.add_argument("--config", help="ssh config file (default ~/.ssh/config)")
    parser.add_argument(
        "--password-for", action="append", default=[], metavar="ALIAS",
        help="prompt for this host's password (repeatable)",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    list_cmd = sub.add_parser("list", help="list a remote directory")
    list_cmd.add_argument("target", metavar="ALIAS:PATH")
    copy_cmd = sub.add_parser("copy", help="copy files or directories between hosts")
    copy_cmd.add_argument("paths", nargs="+", metavar="SRC... DST", help="one or more sources, then the destination")
    copy_cmd.add_argument("--jobs", type=int, default=2, help="copies running at once (default 2)")
    copy_cmd.add_argument("--links", action="store_true", help="copy symlinks as links (directories only)")
    copy_cmd.add_argument(
        "--overwrite", action="store_true", help="replace existing destination files that differ"
    )
    copy_cmd.add_argument("--verify", action="store_true", help="also compare sha256 checksums")
    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "copy" and len(args.paths) < 2:
        parser.error("copy needs at least one source and a destination")
    if args.command == "copy" and args.jobs < 1:
        parser.error("--jobs must be at least 1")
    runner = run_list if args.command == "list" else run_copy
    try:
        return asyncio.run(runner(args)) or 0
    except KeyboardInterrupt:
        print("\npartial files kept; run the same command again to resume", file=sys.stderr)
        return 130
    except (ConnectError, TransferError, ValueError) as exc:
        return fail(str(exc))
    except asyncssh.SFTPError as exc:
        return fail(f"SFTP error: {exc.reason}")
    except (asyncssh.Error, OSError) as exc:
        return fail(f"connection problem: {exc}")


def fail(message: str) -> int:
    """Print a one-line error (no traceback) and return the exit code."""
    if sys.stderr.isatty():  # wipe a half-drawn progress line
        sys.stderr.write("\r" + " " * 70 + "\r")
    print(f"hopsync: error: {message}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
