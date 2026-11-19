"""The optional settings file (settings.toml). It never holds secrets.

This lives outside `core` on purpose: it is application configuration, and
core only ever receives plain arguments.
"""

import os
import sys
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

from .core.sshconfig import list_host_aliases

KNOWN_KEYS = ("hosts", "password_for", "ssh_config", "jobs", "port")
# Words that mean "this key looks like it holds a secret".
SECRET_WORDS = ("password", "passphrase", "secret", "token")

TEMPLATE = """\
# hopsync settings.
#
# NEVER put passwords, passphrases or tokens in this file. hopsync asks for
# them when it starts and keeps them in memory only. A key that looks like a
# secret makes hopsync refuse to start.
#
# Everything here is optional. Command-line options override these values
# (and --password-for adds to password_for).

# Hosts shown in the web UI's host picker, in this order. The first two are
# the defaults for the left and right pane. Default: every Host in your ssh
# config (patterns like "*" are skipped).
# hosts = ["<hpc-alias>", "homeserver"]

# Hosts that need a password (hopsync prompts for them at startup).
# password_for = ["<hpc-alias>"]

# Path to your ssh config. Default: ~/.ssh/config
# ssh_config = "C:/Users/you/.ssh/config"

# How many copies run at the same time. Default: 2
# jobs = 2

# Port for the web UI on 127.0.0.1. Default: 0 (pick a free port)
# port = 0
"""


class SettingsError(ValueError):
    """The settings file cannot be used. The message is shown to the user."""


@dataclass
class Settings:
    hosts: list[str] | None = None  # None: show every alias from the ssh config
    password_for: list[str] = field(default_factory=list)
    ssh_config: str | None = None
    jobs: int | None = None
    port: int | None = None
    warnings: list[str] = field(default_factory=list)


def settings_path(environ=None, windows: bool | None = None) -> Path:
    """Where settings.toml lives on this system."""
    environ = os.environ if environ is None else environ
    windows = (os.name == "nt") if windows is None else windows
    if windows:
        base = Path(environ.get("APPDATA") or Path.home() / "AppData" / "Roaming")
    else:
        base = Path(environ.get("XDG_CONFIG_HOME") or Path.home() / ".config")
    return base / "hopsync" / "settings.toml"


def create_template(path: Path) -> bool:
    """Write the commented template. Returns False (and changes nothing) if the file exists."""
    if path.exists():
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(path, "x", encoding="utf-8") as f:  # "x": never overwrite
            f.write(TEMPLATE)
    except FileExistsError:
        return False
    return True


def load_settings(path: Path | None = None) -> Settings:
    """Read the settings file. A missing file means all defaults."""
    path = path or settings_path()
    try:
        with open(path, "rb") as f:
            data = tomllib.load(f)
    except FileNotFoundError:
        return Settings()
    except (tomllib.TOMLDecodeError, OSError) as error:
        raise SettingsError(f"cannot read {path}: {error}") from None
    return parse_settings(data)


def find_secret_key(data, found=None):
    """First key (at any depth) that looks like it holds a secret, or None."""
    for key, value in data.items():
        lowered = str(key).lower()
        if lowered != "password_for" and any(word in lowered for word in SECRET_WORDS):
            return key
        if isinstance(value, dict):
            found = find_secret_key(value)
            if found:
                return found
    return None


def parse_settings(data: dict) -> Settings:
    # A password in a plain-text file is exactly what we must not encourage,
    # so a suspicious key stops hopsync from starting at all.
    secret_key = find_secret_key(data)
    if secret_key:
        raise SettingsError(
            f"settings.toml must not contain secrets (found key '{secret_key}'); "
            "hopsync asks for passwords at startup"
        )
    settings = Settings()
    for key, value in data.items():
        if key not in KNOWN_KEYS:
            settings.warnings.append(f"settings.toml: unknown key '{key}' (ignored)")
    settings.hosts = _alias_list(data, "hosts")
    settings.password_for = _alias_list(data, "password_for") or []
    settings.ssh_config = _optional(data, "ssh_config", str, "a path in quotes")
    if settings.ssh_config:
        settings.ssh_config = str(Path(settings.ssh_config).expanduser())
    settings.jobs = _optional(data, "jobs", int, "a whole number, 1 or more")
    if settings.jobs is not None and settings.jobs < 1:
        raise SettingsError("settings.toml: 'jobs' must be a whole number, 1 or more")
    settings.port = _optional(data, "port", int, "a whole number from 0 to 65535")
    if settings.port is not None and not 0 <= settings.port <= 65535:
        raise SettingsError("settings.toml: 'port' must be a whole number from 0 to 65535")
    return settings


def _alias_list(data: dict, key: str) -> list[str] | None:
    if key not in data:
        return None
    value = data[key]
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise SettingsError(f'settings.toml: \'{key}\' must be a list of names, like ["a", "b"]')
    return list(dict.fromkeys(value))  # drop duplicates, keep the order


def _optional(data: dict, key: str, kind: type, description: str):
    if key not in data:
        return None
    value = data[key]
    if not isinstance(value, kind) or isinstance(value, bool):
        raise SettingsError(f"settings.toml: '{key}' must be {description}")
    return value


def apply_settings(args, settings: Settings) -> None:
    """Fill in `args` from the settings. Command-line values win."""
    args.config = args.config or settings.ssh_config
    # --password-for on the command line ADDS to the file's list.
    args.password_for = list(dict.fromkeys([*settings.password_for, *args.password_for]))
    if getattr(args, "jobs", None) is None:
        args.jobs = settings.jobs or 2
    if getattr(args, "port", None) is None:
        args.port = settings.port or 0
    args.hosts = settings.hosts


def effective_aliases(hosts: list[str] | None, config_path) -> tuple[list[str], list[str]]:
    """The host list the UI may use, plus warnings about dropped names."""
    available = list_host_aliases(config_path)
    if hosts is None:
        return available, []
    warnings = [
        f"settings.toml: host '{alias}' is not in your ssh config (dropped)"
        for alias in hosts
        if alias not in available
    ]
    return [alias for alias in hosts if alias in available], warnings


def print_warnings(warnings: list[str]) -> None:
    for warning in warnings:
        print(f"hopsync: warning: {warning}", file=sys.stderr)
