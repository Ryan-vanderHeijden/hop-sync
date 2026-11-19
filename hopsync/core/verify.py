"""Remote checksums. The only place a remote command is built."""

import shlex

from .errors import TransferError
from .session import Endpoint


async def remote_sha256(endpoint: Endpoint, path: str) -> str:
    """Run sha256sum on the host and return the hex digest."""
    # shlex.quote makes the path a single, harmless shell word; "--" stops
    # a path starting with "-" from being read as an option.
    command = f"sha256sum -- {shlex.quote(path)}"
    result = await endpoint.conn.run(command, check=False)
    if result.exit_status == 127:
        raise TransferError(f"sha256sum is not available on {endpoint.alias}")
    if result.exit_status != 0:
        stderr = str(result.stderr).strip()
        raise TransferError(f"sha256sum failed on {endpoint.alias}: {stderr}")
    return str(result.stdout).split()[0]
