"""Builds the wheel and looks inside (slow: it runs the real build)."""

import shutil
import subprocess
import zipfile
from pathlib import Path

import pytest

import hopsync

ROOT = Path(__file__).parent.parent
pytestmark = pytest.mark.slow


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv is not installed")
def test_wheel_contains_the_web_files_and_the_command(tmp_path):
    result = subprocess.run(
        ["uv", "build", "--wheel", "--out-dir", str(tmp_path)], cwd=ROOT, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    wheel = next(tmp_path.glob("hopsync-*.whl"))
    assert f"hopsync-{hopsync.__version__}-" in wheel.name  # the version comes from __init__.py
    with zipfile.ZipFile(wheel) as archive:
        names = set(archive.namelist())
        for name in ("index.html", "style.css", "app.js", "api.js", "conflict.js", "dom.js", "format.js", "jobs.js", "pane.js"):
            assert f"hopsync/web/{name}" in names, name
        entry_points = archive.read(next(n for n in names if n.endswith("entry_points.txt"))).decode()
        assert "hopsync = hopsync.cli:main" in entry_points
        metadata = archive.read(next(n for n in names if n.endswith("/METADATA"))).decode()
        assert "Name: hopsync" in metadata and "README" not in "" and "hop-sync" in metadata.lower()
