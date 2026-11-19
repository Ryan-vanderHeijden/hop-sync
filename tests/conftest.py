"""Shared fixtures. The docker hosts start once per test session."""

import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

import asyncssh
import pytest

TESTS_DIR = Path(__file__).parent
HOSTDATA = TESTS_DIR / ".hostdata"
COMPOSE = ["docker", "compose", "-p", "hopsync-test", "-f", str(TESTS_DIR / "docker-compose.yml")]
PASSWORD = "hunter2pass"
PORTS = {"server": 2222, "hpc": 2223}


@dataclass
class Hosts:
    config: Path  # ssh config with aliases `server` and `hpc`
    config_bad_known_hosts: Path  # same, but known_hosts is empty
    password: str


def docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    return subprocess.run(["docker", "info"], capture_output=True).returncode == 0


def write_client_key() -> Path:
    """Generate an ed25519 key; the public half is mounted into the containers."""
    HOSTDATA.mkdir(exist_ok=True)
    key = asyncssh.generate_private_key("ssh-ed25519")
    key_path = HOSTDATA / "client_key"
    key_path.write_bytes(key.export_private_key())
    key_path.chmod(0o600)
    (HOSTDATA / "client_key.pub").write_bytes(key.export_public_key())
    return key_path


def host_public_key(service: str) -> str:
    """Read the container's host key, as 'ssh-ed25519 AAAA...'."""
    out = subprocess.run(
        [*COMPOSE, "exec", "-T", service, "cat", "/etc/ssh/ssh_host_ed25519_key.pub"],
        capture_output=True, text=True, check=True,
    ).stdout
    return " ".join(out.split()[:2])


def write_config(path: Path, key_path: Path, known_hosts: Path) -> None:
    blocks = []
    for alias, port in PORTS.items():
        blocks.append(
            f"Host {alias}\n"
            f"  HostName 127.0.0.1\n"
            f"  Port {port}\n"
            f"  User tester\n"
            f"  IdentityFile {key_path}\n"
            f"  IdentitiesOnly yes\n"
            f"  UserKnownHostsFile {known_hosts}\n"
        )
    path.write_text("\n".join(blocks))


@pytest.fixture(scope="session")
def hosts():
    if not docker_available():
        pytest.skip("docker is not available")
    key_path = write_client_key()
    try:
        subprocess.run([*COMPOSE, "up", "-d", "--build", "--wait"], check=True, capture_output=True)
        known = HOSTDATA / "known_hosts"
        known.write_text(
            "".join(f"[127.0.0.1]:{port} {host_public_key(name)}\n" for name, port in PORTS.items())
        )
        empty = HOSTDATA / "known_hosts_empty"
        empty.write_text("")
        good, bad = HOSTDATA / "ssh_config", HOSTDATA / "ssh_config_bad"
        write_config(good, key_path, known)
        write_config(bad, key_path, empty)
        yield Hosts(good, bad, PASSWORD)
    finally:
        subprocess.run([*COMPOSE, "down", "-v"], capture_output=True)


@pytest.fixture(autouse=True)
def isolated_settings_file(monkeypatch, tmp_path_factory):
    """Never read (or create) the real settings.toml of whoever runs the tests."""
    config_home = tmp_path_factory.mktemp("config-home")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(config_home))
    monkeypatch.setenv("APPDATA", str(config_home))
