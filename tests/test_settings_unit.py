"""settings.toml, the command dispatch, and the host list from settings."""

import argparse
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from test_api_unit import JOB, ORIGIN, PORT, SESSION, TOKEN, fake_connector, fake_copier, fake_resolver

from hopsync import cli
from hopsync.api.app import create_app
from hopsync.api.hub import EventHub
from hopsync.core.jobs import JobManager
from hopsync.core.sessions import SessionPool
from hopsync.settings import (
    SettingsError,
    TEMPLATE,
    apply_settings,
    create_template,
    effective_aliases,
    load_settings,
    parse_settings,
    settings_path,
)


def write(tmp_path, text, name="settings.toml") -> Path:
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


# ---- where the file lives -----------------------------------------------------


def test_settings_path_windows_and_elsewhere(tmp_path):
    assert settings_path({"APPDATA": r"C:\Users\me\AppData\Roaming"}, windows=True).parts[-2:] == ("hopsync", "settings.toml")
    assert "AppData" in str(settings_path({"APPDATA": r"C:\Users\me\AppData\Roaming"}, windows=True))
    assert settings_path({"XDG_CONFIG_HOME": "/x/cfg"}, windows=False) == Path("/x/cfg/hopsync/settings.toml")
    default = settings_path({}, windows=False)
    assert default == Path.home() / ".config" / "hopsync" / "settings.toml"


# ---- reading ----------------------------------------------------------------


def test_missing_file_means_defaults(tmp_path):
    settings = load_settings(tmp_path / "nope.toml")
    assert settings.hosts is None and settings.password_for == [] and settings.jobs is None


def test_every_key_is_read(tmp_path):
    path = write(tmp_path, '''
hosts = ["homeserver", "hpc", "homeserver"]
password_for = ["hpc"]
ssh_config = "~/other/config"
jobs = 3
port = 8123
''')
    settings = load_settings(path)
    assert settings.hosts == ["homeserver", "hpc"]  # order kept, duplicates dropped
    assert settings.password_for == ["hpc"]
    assert settings.ssh_config == str(Path("~/other/config").expanduser())
    assert (settings.jobs, settings.port) == (3, 8123)
    assert settings.warnings == []


@pytest.mark.parametrize(
    "text",
    ["jobs = 0", 'jobs = "two"', "jobs = true", "port = 70000", 'hosts = "hpc"', "hosts = [1, 2]", "ssh_config = 5", "not valid toml ["],
)
def test_bad_values_are_clear_errors(tmp_path, text):
    with pytest.raises(SettingsError):
        load_settings(write(tmp_path, text))


def test_unknown_keys_warn_but_continue(tmp_path):
    settings = load_settings(write(tmp_path, 'colour = "blue"\njobs = 2\n'))
    assert settings.jobs == 2
    assert settings.warnings == ["settings.toml: unknown key 'colour' (ignored)"]


@pytest.mark.parametrize(
    "line",
    ['password = "x"', 'hpc_password = "x"', 'PASSWORD="x"', 'passphrase = "x"', 'key_passphrase = "x"',
     'secret = "x"', 'client_Secret = "x"', 'token = "x"', 'api_token = "x"', '[hpc]\npassword = "x"'],
)
def test_secret_looking_keys_stop_hopsync(tmp_path, line):
    with pytest.raises(SettingsError, match=r"settings.toml must not contain secrets \(found key '.*'\); hopsync asks for passwords at startup"):
        load_settings(write(tmp_path, line))


def test_password_for_is_allowed(tmp_path):
    assert load_settings(write(tmp_path, 'password_for = ["hpc"]')).password_for == ["hpc"]


# ---- merging with the command line ---------------------------------------------


def args_with(**values):
    base = {"config": None, "password_for": [], "jobs": None, "port": None}
    return argparse.Namespace(**{**base, **values})


def test_command_line_overrides_and_merges():
    settings = parse_settings({"ssh_config": "/file/config", "jobs": 4, "port": 9000, "password_for": ["a", "b"], "hosts": ["a"]})
    args = args_with()
    apply_settings(args, settings)
    assert (args.config, args.jobs, args.port, args.password_for, args.hosts) == ("/file/config", 4, 9000, ["a", "b"], ["a"])

    args = args_with(config="/cli/config", jobs=1, port=1234, password_for=["b", "c"])
    apply_settings(args, settings)
    assert (args.config, args.jobs, args.port) == ("/cli/config", 1, 1234)
    assert args.password_for == ["a", "b", "c"]  # the command line ADDS to the file

    args = args_with()
    apply_settings(args, parse_settings({}))
    assert (args.config, args.jobs, args.port, args.hosts) == (None, 2, 0, None)


# ---- the host list ------------------------------------------------------------


def make_ssh_config(tmp_path):
    return write(tmp_path, "Host homeserver\n  HostName a\nHost hpc\n  HostName b\nHost github.com\n  HostName c\nHost *\n  User x\n", "config")


def test_hosts_are_filtered_and_ordered(tmp_path):
    config = make_ssh_config(tmp_path)
    assert effective_aliases(None, config) == (["homeserver", "hpc", "github.com"], [])
    aliases, warnings = effective_aliases(["hpc", "gone", "homeserver"], config)
    assert aliases == ["hpc", "homeserver"]
    assert warnings == ["settings.toml: host 'gone' is not in your ssh config (dropped)"]


def build_client(aliases):
    pool = SessionPool(connector=fake_connector)
    hub = EventHub()
    manager = JobManager(pool, hub.publish, copier=fake_copier, resolver=fake_resolver)
    return TestClient(create_app(manager, pool, aliases, TOKEN, SESSION, PORT, hub), base_url=ORIGIN)


def test_api_uses_the_settings_host_list_in_its_order():
    with build_client(["server", "hpc"]) as client:
        client.get(f"/auth?token={TOKEN}", follow_redirects=False)
        assert [h["alias"] for h in client.get("/api/hosts").json()] == ["server", "hpc"]
        assert client.get("/api/list", params={"host": "locked", "path": "."}).status_code == 400  # not in the list
        other = {"src": {"host": "hpc", "path": "a"}, "dst": {"host": "down", "path": "b"}}
        assert client.post("/api/jobs", json=other).status_code == 400
        assert client.post("/api/jobs", json=JOB).status_code == 201


# ---- the settings command -------------------------------------------------------


def test_template_is_created_once_and_never_overwritten(tmp_path):
    path = tmp_path / "sub" / "settings.toml"
    assert create_template(path) is True
    assert path.read_text(encoding="utf-8") == TEMPLATE
    path.write_text('jobs = 7\n', encoding="utf-8")
    assert create_template(path) is False
    assert path.read_text(encoding="utf-8") == "jobs = 7\n"
    # the template itself is valid and changes nothing
    assert load_settings(write(tmp_path, TEMPLATE, "t.toml")).hosts is None


def test_settings_command_creates_the_file_then_leaves_it_alone(capsys):
    assert cli.main(["settings"]) == 0
    first = capsys.readouterr().out
    path = settings_path()
    assert str(path) in first and "created" in first and path.exists()
    path.write_text("jobs = 5\n", encoding="utf-8")
    assert cli.main(["settings"]) == 0
    assert "not changed" in capsys.readouterr().out
    assert path.read_text(encoding="utf-8") == "jobs = 5\n"


def test_a_settings_file_with_a_secret_stops_commands_cleanly(capsys):
    path = settings_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('password = "oops"\n', encoding="utf-8")
    assert cli.main(["list", "hpc:."]) == 1
    assert "must not contain secrets" in capsys.readouterr().err


# ---- dispatch ---------------------------------------------------------------------


def test_no_subcommand_means_serve():
    _, args = cli.parse_args([])
    assert args.command == "serve" and args.port is None and args.no_browser is False
    _, args = cli.parse_args(["--config", "x", "--password-for", "hpc"])
    assert (args.command, args.config, args.password_for) == ("serve", "x", ["hpc"])
    _, args = cli.parse_args(["serve", "--no-browser"])
    assert args.command == "serve" and args.no_browser


def test_main_with_no_arguments_runs_serve_with_the_settings(monkeypatch):
    path = settings_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('password_for = ["hpc"]\nhosts = ["hpc"]\njobs = 3\nport = 4321\n', encoding="utf-8")
    seen = []

    async def fake_serve(args):
        seen.append(args)
        return 0

    monkeypatch.setattr(cli, "run_serve", fake_serve)
    assert cli.main([]) == 0
    args = seen[0]
    assert (args.command, args.password_for, args.hosts, args.jobs, args.port) == ("serve", ["hpc"], ["hpc"], 3, 4321)


def test_version_flag(capsys):
    with pytest.raises(SystemExit) as info:
        cli.main(["--version"])
    assert info.value.code == 0
    assert capsys.readouterr().out.startswith("hopsync 0.")


def test_keyboard_interrupt_message_and_exit_code(monkeypatch, capsys):
    from hopsync import cli

    def interrupted(coro):
        coro.close()
        raise KeyboardInterrupt

    monkeypatch.setattr(cli.asyncio, "run", interrupted)
    assert cli.main(["copy", "a:x", "b:y"]) == 130
    assert "partial files kept; run the same command again to resume" in capsys.readouterr().err
