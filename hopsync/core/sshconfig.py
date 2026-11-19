"""A tiny reader for the Host names in an ssh config file."""

import glob
import re
from pathlib import Path

_HOST_LINE = re.compile(r"^\s*Host(?:\s+|\s*=\s*)(.+)$", re.IGNORECASE)
_INCLUDE_LINE = re.compile(r"^\s*Include(?:\s+|\s*=\s*)(.+)$", re.IGNORECASE)
_MAX_INCLUDE_DEPTH = 5


def list_host_aliases(config_path: str | Path | None = None) -> list[str]:
    """Host aliases from the config, skipping wildcard/negated patterns.

    Defaults to ~/.ssh/config. A missing file gives an empty list.
    """
    path = Path(config_path) if config_path else Path.home() / ".ssh" / "config"
    aliases: list[str] = []
    _read(path, aliases, depth=0)
    return aliases


def _read(path: Path, aliases: list, depth: int) -> None:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return
    for line in text.splitlines():
        line = line.split("#", 1)[0]  # drop comments
        host_match = _HOST_LINE.match(line)
        include_match = _INCLUDE_LINE.match(line)
        if host_match:
            _add_aliases(host_match.group(1), aliases)
        elif include_match and depth < _MAX_INCLUDE_DEPTH:
            _read_includes(include_match.group(1), path, aliases, depth)


def _add_aliases(names: str, aliases: list) -> None:
    for name in names.split():
        name = name.strip('"')
        if not name or any(char in name for char in "*?!") or name in aliases:
            continue
        aliases.append(name)


def _read_includes(patterns: str, current: Path, aliases: list, depth: int) -> None:
    # Like ssh, relative Include paths are relative to the user's ~/.ssh;
    # we use the directory of the including file, which is the same thing.
    for pattern in patterns.split():
        pattern = str(Path(pattern.strip('"')).expanduser())
        if not Path(pattern).is_absolute():
            pattern = str(current.parent / pattern)
        for found in sorted(glob.glob(pattern)):
            _read(Path(found), aliases, depth + 1)
