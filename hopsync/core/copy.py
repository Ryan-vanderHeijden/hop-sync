"""Copy a file or a directory: picks copy_file or copy_tree by source type."""

import stat

from .session import Endpoint
from .transfer import copy_file
from .tree import TreeResult, copy_tree


async def copy_path(
    src: Endpoint,
    src_path: str,
    dst: Endpoint,
    dst_path: str,
    on_event,
    *,
    links: bool = False,
    verify: bool = False,
    overwrite: bool = False,
    chunk_size: int = 4 * 1024 * 1024,
    created_dirs: set | None = None,
) -> TreeResult | None:
    """Copy src_path to dst_path (already resolved). Returns a TreeResult for
    directories and None for single files (which raise on failure)."""
    attrs = await src.sftp.stat(src_path)
    if stat.S_ISDIR(attrs.permissions or 0):
        return await copy_tree(
            src, src_path, dst, dst_path, on_event, links=links, verify=verify,
            overwrite=overwrite, created_dirs=created_dirs, chunk_size=chunk_size,
        )
    await copy_file(
        src, src_path, dst, dst_path, on_event, verify=verify, overwrite=overwrite, chunk_size=chunk_size
    )
    return None
