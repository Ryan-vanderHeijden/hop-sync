from hopsync.core.sshconfig import list_host_aliases


def test_aliases_skip_patterns_and_comments(tmp_path):
    config = tmp_path / "config"
    config.write_text(
        """
# a comment
Host hpc
    HostName 10.0.0.1

host HomeServer  home   # trailing comment
    User me

Host *
    ServerAliveInterval 30

Host *.example.com !bad ok? "quoted"
    User x

Host hpc
    Port 22
"""
    )
    assert list_host_aliases(config) == ["hpc", "HomeServer", "home", "quoted"]


def test_include_is_followed(tmp_path):
    (tmp_path / "extra.conf").write_text("Host included\n  HostName h\n")
    config = tmp_path / "config"
    config.write_text("Host first\nInclude extra.conf\nHost last\n")
    assert list_host_aliases(config) == ["first", "included", "last"]


def test_missing_file_gives_empty_list(tmp_path):
    assert list_host_aliases(tmp_path / "nope") == []
