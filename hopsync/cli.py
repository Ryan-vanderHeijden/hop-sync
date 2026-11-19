"""Command-line interface: a thin layer over hopsync.core."""

import argparse
import asyncio
import getpass
import sys
import time
from datetime import datetime

import asyncssh

from .core.events import TransferFinished, TransferProgress
from .core.listing import list_dir
from .core.paths import parse_remote
from .core.session import ConnectError, KeyNeedsPassphrase, connect
from .core.transfer import TransferError, copy_file, resolve_destination


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

    def __call__(self, event) -> None:
        if isinstance(event, TransferProgress):
            now = time.monotonic()
            if now - self.last_draw >= 0.1:
                self.last_draw = now
                self.draw(event.bytes_done, event.total_bytes, now - self.start)

    def draw(self, done: int, total: int, elapsed: float) -> None:
        rate = done / elapsed if elapsed > 0 else 0
        percent = 100 * done / total if total else 100
        eta = format_eta((total - done) / rate) if rate > 0 else "--:--"
        line = (
            f"{percent:5.1f}%  {human_size(done)}/{human_size(total)}  "
            f"{rate / 1e6:.1f} MB/s  ETA {eta}"
        )
        sys.stderr.write("\r" + line.ljust(70))
        sys.stderr.flush()


def print_summary(event: TransferFinished) -> None:
    """Final line: total size, time and average throughput."""
    rate = event.bytes_done / event.seconds / 1e6 if event.seconds > 0 else 0
    sys.stderr.write("\r" + " " * 70 + "\r")
    print(f"copied {human_size(event.bytes_done)} in {event.seconds:.1f}s ({rate:.1f} MB/s average)")


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


async def run_copy(args) -> None:
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
            dst_path = await resolve_destination(dst_sftp, src.path, dst.path)
            bar = ProgressBar()
            finished = []

            def on_event(event) -> None:
                bar(event)
                if isinstance(event, TransferFinished):
                    finished.append(event)

            await copy_file(src_sftp, src.path, dst_sftp, dst_path, on_event)
            print_summary(finished[0])
    finally:
        for conn in connections:
            conn.close()
            await conn.wait_closed()


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
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    runner = run_list if args.command == "list" else run_copy
    try:
        asyncio.run(runner(args))
    except KeyboardInterrupt:
        print("\ninterrupted", file=sys.stderr)
        return 130
    except (ConnectError, TransferError, ValueError) as exc:
        return fail(str(exc))
    except asyncssh.SFTPError as exc:
        return fail(f"SFTP error: {exc.reason}")
    except (asyncssh.Error, OSError) as exc:
        return fail(f"connection problem: {exc}")
    return 0


def fail(message: str) -> int:
    """Print a one-line error (no traceback) and return the exit code."""
    if sys.stderr.isatty():  # wipe a half-drawn progress line
        sys.stderr.write("\r" + " " * 70 + "\r")
    print(f"hopsync: error: {message}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
