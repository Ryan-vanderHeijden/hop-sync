"""1 GiB transfer: checks the data survives and memory stays small."""

import asyncio
import sys
import time

import pytest

from hopsync.core.session import Endpoint, connect
from hopsync.core.transfer import copy_file

pytestmark = [pytest.mark.slow, pytest.mark.integration]

# "Flat memory": the copy may add at most this much on top of what the
# process used before it started (pytest + asyncssh are ~70 MB on their own).
RSS_GROWTH_LIMIT_MB = 50


def current_rss_mb() -> float:
    """Resident memory of this process, read from /proc (Linux only)."""
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    raise RuntimeError("VmRSS not found")


async def watch_rss(peak: list) -> None:
    while True:
        peak[0] = max(peak[0], current_rss_mb())
        await asyncio.sleep(0.2)


async def test_one_gib_copy(hosts):
    if not sys.platform.startswith("linux"):
        pytest.skip("needs /proc")
    hpc = await connect("hpc", password=hosts.password, config_path=hosts.config)
    server = await connect("server", config_path=hosts.config)
    try:
        await hpc.run("head -c 1073741824 /dev/urandom > big.bin", check=True)
        baseline = current_rss_mb()
        peak = [baseline]
        watcher = asyncio.create_task(watch_rss(peak))
        start = time.monotonic()
        async with hpc.start_sftp_client() as s, server.start_sftp_client() as d:
            n = await copy_file(Endpoint("hpc", hpc, s), "big.bin", Endpoint("server", server, d), "big.bin", lambda e: None)
        seconds = time.monotonic() - start
        watcher.cancel()

        print(f"\n1 GiB in {seconds:.1f}s = {n / seconds / 1e6:.0f} MB/s, peak RSS {peak[0]:.0f} MB (baseline {baseline:.0f} MB)")
        assert n == 1073741824
        src_hash = (await hpc.run("sha256sum big.bin", check=True)).stdout.split()[0]
        dst_hash = (await server.run("sha256sum big.bin", check=True)).stdout.split()[0]
        assert src_hash == dst_hash
        assert peak[0] - baseline < RSS_GROWTH_LIMIT_MB
    finally:
        for conn in (hpc, server):
            conn.close()
            await conn.wait_closed()
