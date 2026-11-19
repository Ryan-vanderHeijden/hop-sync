"""Command-line interface: a thin layer over hopsync.core."""

import argparse
import asyncio
import getpass
import stat
import sys
import time
from datetime import datetime

import asyncssh

from .core.events import (
    EntryFailed,
    EntrySkipped,
    TransferFinished,
    TransferProgress,
    TransferStarted,
    TreeProgress,
    Verifying,
)
from .core.listing import list_dir
from .core.paths import parse_remote
from .core.session import ConnectError, Endpoint, KeyNeedsPassphrase, connect
from .core.transfer import TransferError, copy_file, resolve_destination
from .core.tree import copy_tree


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


class ProgressBar:
    """One status line, rewritten with \\r, at most ~10 times a second."""

    def __init__(self) -> None:
        self.start = time.monotonic()
        self.last_draw = 0.0
        self.resumed_from = 0

    def __call__(self, event) -> None:
        if isinstance(event, TransferStarted):
            self.resumed_from = event.resumed_from
            if event.resumed_from:
                print(f"resuming from {human_size(event.resumed_from)}", file=sys.stderr)
        elif isinstance(event, Verifying):
            self.clear()
            print(f"verifying ({event.method})...", file=sys.stderr)
        elif isinstance(event, TransferProgress):
            now = time.monotonic()
            if now - self.last_draw >= 0.1:
                self.last_draw = now
                self.draw(event.bytes_done, event.total_bytes, now - self.start)

    def clear(self) -> None:
        if sys.stderr.isatty():
            sys.stderr.write("\r" + " " * 70 + "\r")

    def draw(self, done: int, total: int, elapsed: float) -> None:
        # Speed and ETA count only bytes moved in this run, not the resumed prefix.
        rate = (done - self.resumed_from) / elapsed if elapsed > 0 else 0
        percent = 100 * done / total if total else 100
        eta = format_eta((total - done) / rate) if rate > 0 else "--:--"
        line = (
            f"{percent:5.1f}%  {human_size(done)}/{human_size(total)}  "
            f"{rate / 1e6:.1f} MB/s  ETA {eta}"
        )
        sys.stderr.write("\r" + line.ljust(70))
        sys.stderr.flush()


class TreeBar:
    """Overall progress line for a directory copy, plus one line per entry."""

    def __init__(self) -> None:
        self.start = time.monotonic()
        self.last_draw = 0.0
        self.tree = None  # latest TreeProgress
        self.current = 0  # bytes of the file being copied right now
        self.last_file = 0
        self.moved = 0  # bytes sent in this run (skipped/resumed bytes excluded)

    def __call__(self, event) -> None:
        if isinstance(event, TransferStarted):
            self.current = self.last_file = event.resumed_from
        elif isinstance(event, TransferProgress):
            self.moved += event.bytes_done - self.last_file
            self.current = self.last_file = event.bytes_done
            self.draw()
        elif isinstance(event, TransferFinished):
            self.current = 0
            self.print_line(f"copied  {event.src} ({human_size(event.bytes_done)})")
        elif isinstance(event, TreeProgress):
            self.tree = event
            self.draw(force=event.walk_complete)
        elif isinstance(event, EntrySkipped):
            self.print_line(f"skipped {event.path} ({event.reason})")
        elif isinstance(event, EntryFailed):
            self.current = 0
            self.print_line(f"FAILED  {event.path}: {event.message}")

    def print_line(self, text: str) -> None:
        """Print a line without leaving a half-drawn progress line behind."""
        if sys.stderr.isatty():
            sys.stderr.write("\r" + " " * 78 + "\r")
        print(text, file=sys.stderr)
        self.last_draw = 0.0  # redraw the bar on the next event

    def draw(self, force: bool = False) -> None:
        now = time.monotonic()
        if self.tree is None or (not force and now - self.last_draw < 0.1):
            return
        self.last_draw = now
        t = self.tree
        done = t.bytes_done + self.current
        elapsed = now - self.start
        rate = self.moved / elapsed if elapsed > 0 else 0
        percent = 100 * done / t.bytes_found if t.bytes_found else 100
        plus = "" if t.walk_complete else "+"  # "+" = the total may still grow
        eta = "--:--"
        if t.walk_complete and rate > 0:
            eta = format_eta((t.bytes_found - done) / rate)
        line = (
            f"{percent:5.1f}%  {human_size(done)}/{human_size(t.bytes_found)}{plus}  "
            f"{t.files_done}/{t.files_found}{plus} files  {rate / 1e6:.1f} MB/s  ETA {eta}"
        )
        sys.stderr.write("\r" + line.ljust(78))
        sys.stderr.flush()

    def clear(self) -> None:
        if sys.stderr.isatty():
            sys.stderr.write("\r" + " " * 78 + "\r")


def print_summary(event: TransferFinished, resumed_from: int) -> None:
    """Final line: total size, time and average throughput of this run."""
    moved = event.bytes_done - resumed_from
    rate = moved / event.seconds / 1e6 if event.seconds > 0 else 0
    print(f"copied {human_size(event.bytes_done)} in {event.seconds:.1f}s ({rate:.1f} MB/s average)")


def print_tree_summary(result, seconds: float) -> None:
    """Final lines for a directory copy; failures are listed again here."""
    rate = result.bytes_copied / seconds / 1e6 if seconds > 0 else 0
    print(
        f"{result.copied} copied, {result.skipped} skipped, {result.failed} failed; "
        f"{human_size(result.bytes_copied)} in {seconds:.1f}s ({rate:.1f} MB/s average)"
    )
    for path, message in result.failures:
        print(f"  failed: {path}: {message}", file=sys.stderr)


async def open_connection(alias: str, args, passwords: dict):
    """Connect, asking for a key passphrase once if the key needs one."""
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


async def run_copy(args) -> int:
    """Copy a file or directory. Returns the exit code."""
    src, dst = parse_remote(args.src), parse_remote(args.dst)
    if src.host == dst.host:
        raise TransferError("source and destination are on the same host; copy on the host itself")
    passwords = ask_passwords(args)
    connections = []
    try:
        src_conn = await open_connection(src.host, args, passwords)
        connections.append(src_conn)
        dst_conn = await open_connection(dst.host, args, passwords)
        connections.append(dst_conn)
        async with src_conn.start_sftp_client() as src_sftp, dst_conn.start_sftp_client() as dst_sftp:
            src_end = Endpoint(src.host, src_conn, src_sftp)
            dst_end = Endpoint(dst.host, dst_conn, dst_sftp)
            dst_path = await resolve_destination(dst_sftp, src.path, dst.path)
            src_attrs = await src_sftp.stat(src.path)
            if stat.S_ISDIR(src_attrs.permissions or 0):
                return await copy_directory(args, src_end, src.path, dst_end, dst_path)
            return await copy_single_file(args, src_end, src.path, dst_end, dst_path)
    finally:
        for conn in connections:
            conn.close()
            await conn.wait_closed()


async def copy_single_file(args, src_end, src_path, dst_end, dst_path) -> int:
    bar = ProgressBar()
    finished = []

    def on_event(event) -> None:
        bar(event)
        if isinstance(event, TransferFinished):
            finished.append(event)

    await copy_file(src_end, src_path, dst_end, dst_path, on_event, verify=args.verify)
    bar.clear()
    print_summary(finished[0], bar.resumed_from)
    return 0


async def copy_directory(args, src_end, src_path, dst_end, dst_path) -> int:
    bar = TreeBar()
    started = time.monotonic()
    result = await copy_tree(
        src_end, src_path, dst_end, dst_path, bar, links=args.links, verify=args.verify
    )
    bar.clear()
    print_tree_summary(result, time.monotonic() - started)
    return 1 if result.failed else 0


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
    copy_cmd = sub.add_parser("copy", help="copy one file between hosts")
    copy_cmd.add_argument("src", metavar="SRC_ALIAS:PATH")
    copy_cmd.add_argument("dst", metavar="DST_ALIAS:PATH")
    copy_cmd.add_argument("--links", action="store_true", help="copy symlinks as links (directories only)")
    copy_cmd.add_argument("--verify", action="store_true", help="also compare sha256 checksums")
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    runner = run_list if args.command == "list" else run_copy
    try:
        return asyncio.run(runner(args)) or 0
    except KeyboardInterrupt:
        print("\ninterrupted" + KEPT_HINT.get(args.command, ""), file=sys.stderr)
        return 130
    except (ConnectError, TransferError, ValueError) as exc:
        return fail(str(exc))
    except asyncssh.SFTPError as exc:
        return fail(f"SFTP error: {exc.reason}")
    except (asyncssh.Error, OSError) as exc:
        return fail(f"connection problem: {exc}" + KEPT_HINT.get(args.command, ""))


KEPT_HINT = {"copy": "; partial file kept; run the same command again to resume"}


def fail(message: str) -> int:
    """Print a one-line error (no traceback) and return the exit code."""
    if sys.stderr.isatty():  # wipe a half-drawn progress line
        sys.stderr.write("\r" + " " * 70 + "\r")
    print(f"hopsync: error: {message}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
