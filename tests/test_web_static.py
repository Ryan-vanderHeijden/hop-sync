"""The static UI: content types, security headers, and no inline code."""

import mimetypes
import re
import shutil
import subprocess
from html.parser import HTMLParser
from pathlib import Path

import pytest

import test_api_unit as api_fixtures
from hopsync.api.app import create_app
from hopsync.api.hub import EventHub
from hopsync.core.jobs import JobManager
from hopsync.core.sessions import SessionPool

from fastapi.testclient import TestClient

WEB = Path(__file__).parent.parent / "hopsync" / "web"
PORT = api_fixtures.PORT
MODULES = ["api", "app", "dom", "format", "jobs", "pane"]


def make_client():
    pool = SessionPool(connector=api_fixtures.fake_connector)
    hub = EventHub()
    manager = JobManager(pool, hub.publish, copier=api_fixtures.fake_copier, resolver=api_fixtures.fake_resolver)
    app = create_app(manager, pool, api_fixtures.ALIASES, api_fixtures.TOKEN, api_fixtures.SESSION, PORT, hub)
    return TestClient(app, base_url=api_fixtures.ORIGIN)


@pytest.fixture
def signed_in():
    with make_client() as client:
        client.get(f"/auth?token={api_fixtures.TOKEN}", follow_redirects=False)
        yield client


def test_content_types(signed_in):
    assert signed_in.get("/").headers["content-type"].startswith("text/html")
    assert signed_in.get("/style.css").headers["content-type"].startswith("text/css")
    for name in MODULES:
        response = signed_in.get(f"/{name}.js")
        assert response.status_code == 200, name
        assert response.headers["content-type"].startswith("text/javascript"), name


def test_js_type_survives_a_windows_style_registry(monkeypatch):
    # Pretend the Windows registry said .js is plain text, then build the app.
    mimetypes.add_type("text/plain", ".js")
    with make_client() as client:
        client.get(f"/auth?token={api_fixtures.TOKEN}", follow_redirects=False)
        assert client.get("/app.js").headers["content-type"].startswith("text/javascript")


def check_security_headers(response):
    headers = response.headers
    csp = headers["content-security-policy"]
    assert "default-src 'self'" in csp and "script-src 'self'" in csp and "style-src 'self'" in csp
    assert f"connect-src 'self' ws://127.0.0.1:{PORT} ws://localhost:{PORT}" in csp
    assert "frame-ancestors 'none'" in csp and "base-uri 'none'" in csp and "form-action 'self'" in csp
    assert headers["x-frame-options"] == "DENY"
    assert headers["x-content-type-options"] == "nosniff"
    assert headers["referrer-policy"] == "no-referrer"


def test_security_headers_on_every_kind_of_response(signed_in):
    for path in ("/", "/app.js", "/style.css", "/api/hosts", "/api/jobs/nothere", "/nothing-here"):
        check_security_headers(signed_in.get(path))
    check_security_headers(signed_in.get("/api/hosts", headers={"host": "evil.example"}))  # 400
    check_security_headers(signed_in.get(f"/auth?token={api_fixtures.TOKEN}", follow_redirects=False))  # 303
    check_security_headers(signed_in.get("/auth?token=wrong"))  # 403
    check_security_headers(signed_in.post("/api/jobs", json={}, headers={"origin": "http://evil.example"}))  # 403
    with make_client() as anonymous:
        check_security_headers(anonymous.get("/"))  # 401 hint page
        check_security_headers(anonymous.get("/api/hosts"))  # 401


class InlineFinder(HTMLParser):
    """Collects things a content security policy would block (or that leak)."""

    def __init__(self):
        super().__init__()
        self.problems = []
        self.in_script = False

    def handle_starttag(self, tag, attrs):
        names = dict(attrs)
        if "style" in names:
            self.problems.append(f"style attribute on <{tag}>")
        if tag == "style":
            self.problems.append("<style> tag")
        for name in names:
            if name.startswith("on"):
                self.problems.append(f"{name} handler on <{tag}>")
        for name in ("src", "href"):
            value = names.get(name) or ""
            if re.match(r"(https?:)?//", value):
                self.problems.append(f"external {name}: {value}")
        if tag == "script":
            self.in_script = True
            if not names.get("src"):
                self.problems.append("inline <script>")

    def handle_endtag(self, tag):
        if tag == "script":
            self.in_script = False

    def handle_data(self, data):
        if self.in_script and data.strip():
            self.problems.append("script content")


def test_index_html_has_no_inline_code_or_external_resources():
    finder = InlineFinder()
    finder.feed((WEB / "index.html").read_text(encoding="utf-8"))
    assert finder.problems == []


def test_front_end_sources_never_parse_remote_data_as_html_or_load_external_files():
    for path in [*WEB.glob("*.js"), WEB / "style.css", WEB / "index.html"]:
        text = path.read_text(encoding="utf-8")
        for forbidden in ("innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval(", "@import"):
            assert forbidden not in text, f"{path.name} uses {forbidden}"
        assert not re.search(r"https?://", text), f"{path.name} mentions an external URL"
        assert "url(" not in text or path.suffix != ".css"
        assert "setAttribute(\"style\"" not in text


node = shutil.which("node")


@pytest.mark.skipif(node is None, reason="node is not installed")
def test_javascript_modules_pass_a_syntax_check_and_helper_tests():
    for path in WEB.glob("*.js"):
        result = subprocess.run([node, "--check", str(path)], capture_output=True, text=True)
        assert result.returncode == 0, f"{path.name}: {result.stderr}"
    for name in ("format.test.mjs", "ui.test.mjs"):
        test_file = Path(__file__).parent / "web" / name
        result = subprocess.run([node, "--test", str(test_file)], capture_output=True, text=True)
        assert result.returncode == 0, result.stdout + result.stderr
