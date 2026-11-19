import pytest

from hopsync.core.paths import RemotePath, parse_remote


@pytest.mark.parametrize(
    "spec, host, path",
    [
        ("hpc:/data/x.bin", "hpc", "/data/x.bin"),
        ("hpc:~/data//sub/../x", "hpc", "data/x"),
        ("hpc:~", "hpc", "."),
        ("hpc:", "hpc", "."),
        ("hpc:rel/dir/", "hpc", "rel/dir"),
        ("hpc:a:b", "hpc", "a:b"),  # only the first colon splits
    ],
)
def test_parse(spec, host, path):
    assert parse_remote(spec) == RemotePath(host, path)


@pytest.mark.parametrize("spec", ["nocolon", ":/path"])
def test_parse_errors(spec):
    with pytest.raises(ValueError):
        parse_remote(spec)
