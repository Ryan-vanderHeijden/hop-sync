"""The web API with fake SSH connections (no network, no docker)."""

import logging
from types import SimpleNamespace

import asyncssh
import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from hopsync.api.app import create_app
from hopsync.api.hub import EventHub
from hopsync.api.serve import build_uvicorn_config, pick_free_port
from hopsync.cli import build_parser
from hopsync.core.events import EntryFailed, EntrySkipped, TransferProgress, TransferStarted
from hopsync.core.jobs import JobManager
from hopsync.core.session import AuthFailed, HostKeyError, KeyNeedsPassphrase
from hopsync.core.sessions import SessionPool

PORT = 51234
TOKEN = "token-SECRET-value"
SESSION = "session-SECRET-value"
PASSWORD = "hunter2-SECRET"
PASSPHRASE = "pp-SECRET"
ORIGIN = f"http://127.0.0.1:{PORT}"
ALIASES = ["hpc", "server", "locked", "needpass", "badkey", "down"]


class ApiSFTP:
    """Pretends to be an SFTP channel for listings."""

    async def readdir(self, path):
        if path == "missing":
            raise asyncssh.SFTPNoSuchFile("no such file")
        if path == "denied":
            raise asyncssh.SFTPPermissionDenied("denied")
        attrs = SimpleNamespace(permissions=0o100644, size=5, mtime=100)
        dir_attrs = SimpleNamespace(permissions=0o040755, size=0, mtime=50)
        return [
            SimpleNamespace(filename="file.txt", attrs=attrs),
            SimpleNamespace(filename="adir", attrs=dir_attrs),
            SimpleNamespace(filename=".", attrs=dir_attrs),
        ]

    async def realpath(self, path):
        return "/home/tester/" + path

    async def stat(self, path):
        return SimpleNamespace(permissions=0o100644, size=0, mtime=0)


class ApiConn:
    closed = False

    def is_closed(self):
        return self.closed

    def close(self):
        self.closed = True

    async def wait_closed(self):
        pass

    async def start_sftp_client(self):
        return ApiSFTP()


async def fake_connector(alias, *, password=None, passphrase=None, config_path=None):
    errors = {
        "locked": AuthFailed("authentication to 'locked' failed (denied)", "locked"),
        "needpass": KeyNeedsPassphrase("the private key for 'needpass' needs a (correct) passphrase", "needpass"),
        "badkey": HostKeyError("host key for 'badkey' is not trusted"),
        "down": ConnectionRefusedError("connect to down failed"),
    }
    if alias in errors:
        raise errors[alias]
    return ApiConn()


async def fake_copier(src, src_path, dst, dst_path, on_event, **kw):
    on_event(TransferStarted(src_path, dst_path, 10))
    on_event(TransferProgress(src_path, dst_path, 10, 10))
    on_event(EntrySkipped("x.txt", "already copied"))
    on_event(EntryFailed("y.txt", "boom"))
    return SimpleNamespace(failed=1)  # so the job ends as "failed" and can be retried


async def fake_resolver(sftp, src_path, dst_path, **kw):
    return dst_path


@pytest.fixture
def client():
    pool = SessionPool(connector=fake_connector)
    pool.set_credentials("hpc", password=PASSWORD, passphrase=PASSPHRASE)
    hub = EventHub()
    manager = JobManager(pool, hub.publish, copier=fake_copier, resolver=fake_resolver)
    app = create_app(manager, pool, ALIASES, TOKEN, SESSION, PORT, hub)
    # `with` keeps ONE event loop alive, which the job tasks need.
    with TestClient(app, base_url=ORIGIN) as test_client:
        yield test_client


@pytest.fixture
def signed_in(client):
    response = client.get(f"/auth?token={TOKEN}", follow_redirects=False)
    assert response.status_code == 303
    return client


def wait_for_state(client, job_id, attempt=1):
    """Poll until the job has finished (the fake copier fails it right away)."""
    import time

    for _ in range(300):
        job = client.get(f"/api/jobs/{job_id}").json()
        if job["attempt"] == attempt and job["state"] in ("done", "failed"):
            return job
        time.sleep(0.01)
    raise AssertionError("job did not finish")


def ws_headers(**extra):
    # TestClient does not take the Host header from base_url for WebSockets.
    return {"host": f"127.0.0.1:{PORT}", "origin": ORIGIN, "cookie": f"hopsync_session={SESSION}", **extra}


JOB = {"src": {"host": "hpc", "path": "data/x"}, "dst": {"host": "server", "path": "out"}}


# ---- sign-in --------------------------------------------------------------


def test_no_cookie_gives_401_and_hint_page(client):
    assert client.get("/api/hosts").status_code == 401
    page = client.get("/")
    assert page.status_code == 401 and "link printed in the terminal" in page.text


def test_wrong_token_is_403_without_cookie(client):
    response = client.get("/auth?token=nope", follow_redirects=False)
    assert response.status_code == 403
    assert "set-cookie" not in response.headers
    assert client.get("/auth", follow_redirects=False).status_code == 403


def test_right_token_sets_strict_httponly_cookie_that_works(client):
    response = client.get(f"/auth?token={TOKEN}", follow_redirects=False)
    assert response.status_code == 303 and response.headers["location"] == "/"
    cookie = response.headers["set-cookie"].lower()
    assert "httponly" in cookie and "samesite=strict" in cookie and "path=/" in cookie
    assert "secure" not in cookie
    assert client.get("/api/hosts").status_code == 200
    assert "hopsync" in client.get("/").text  # the placeholder page, behind the cookie
    # a second sign-in (second tab) still works: the token stays valid
    assert client.get(f"/auth?token={TOKEN}", follow_redirects=False).status_code == 303


def test_wrong_cookie_value_is_rejected(client):
    client.cookies.set("hopsync_session", "wrong", domain="127.0.0.1")
    assert client.get("/api/hosts").status_code == 401


# ---- host / origin / cors --------------------------------------------------


def test_foreign_host_header_is_400_even_when_signed_in(signed_in):
    for host in (f"evil.example:{PORT}", "127.0.0.1", f"127.0.0.1:{PORT + 1}"):
        assert signed_in.get("/api/hosts", headers={"host": host}).status_code == 400
    assert signed_in.get("/api/hosts", headers={"host": f"localhost:{PORT}"}).status_code == 200
    # also before sign-in, and for /auth
    assert signed_in.get(f"/auth?token={TOKEN}", headers={"host": "evil.example"}).status_code == 400


def test_post_and_delete_with_foreign_origin_are_403(signed_in):
    evil = {"origin": "http://evil.example"}
    assert signed_in.post("/api/jobs", json=JOB, headers=evil).status_code == 403
    assert signed_in.delete("/api/jobs/abc", headers=evil).status_code == 403
    assert signed_in.post("/api/jobs", json=JOB, headers={"origin": ORIGIN}).status_code == 201
    assert signed_in.post("/api/jobs", json=JOB, headers={"origin": f"http://localhost:{PORT}"}).status_code == 201
    assert signed_in.post("/api/jobs", json=JOB).status_code == 201  # no Origin header: allowed


def test_no_cors_headers_anywhere(signed_in):
    headers = {"origin": "http://evil.example"}
    responses = [
        signed_in.get("/api/hosts", headers=headers),
        signed_in.get("/", headers=headers),
        signed_in.post("/api/jobs", json=JOB, headers=headers),
        signed_in.options("/api/hosts", headers={**headers, "access-control-request-method": "GET"}),
    ]
    for response in responses:
        assert not any(name.lower().startswith("access-control") for name in response.headers)


# ---- websocket -------------------------------------------------------------


@pytest.mark.parametrize(
    "headers",
    [
        {"host": f"127.0.0.1:{PORT}", "cookie": f"hopsync_session={SESSION}"},  # Origin missing
        {"host": f"127.0.0.1:{PORT}", "origin": "http://evil.example", "cookie": f"hopsync_session={SESSION}"},
        {"host": f"127.0.0.1:{PORT}", "origin": ORIGIN},  # no cookie
        {"host": f"127.0.0.1:{PORT}", "origin": ORIGIN, "cookie": "hopsync_session=wrong"},
        {"host": "evil.example", "origin": ORIGIN, "cookie": f"hopsync_session={SESSION}"},
    ],
)
def test_websocket_rejects_bad_requests(client, headers):
    with pytest.raises(WebSocketDisconnect) as info:
        with client.websocket_connect("/api/events", headers=headers):
            pass
    assert info.value.code == 1008


def test_websocket_sends_snapshot_then_job_and_entry_events(signed_in):
    existing = signed_in.post("/api/jobs", json=JOB).json()
    with signed_in.websocket_connect("/api/events", headers=ws_headers()) as ws:
        snapshot = ws.receive_json()
        assert snapshot["type"] == "snapshot"
        assert [job["id"] for job in snapshot["jobs"]] == [existing["id"]]

        new_id = signed_in.post("/api/jobs", json=JOB).json()["id"]
        seen = []
        for _ in range(50):  # bounded, so a bug fails the test instead of hanging it
            message = ws.receive_json()
            seen.append(message)
            if message["type"] == "job" and message["job"]["id"] == new_id and message["job"]["state"] == "failed":
                break
    kinds = {(m["type"], m.get("kind")) for m in seen if m.get("job_id", m.get("job", {}).get("id")) == new_id}
    assert ("job", None) in kinds and ("entry", "skipped") in kinds and ("entry", "failed") in kinds
    assert next(m for m in seen if m["type"] == "entry" and m["kind"] == "failed")["reason"] == "boom"


# ---- endpoints -------------------------------------------------------------


def test_hosts_endpoint(signed_in):
    signed_in.get("/api/list?host=hpc&path=.")  # connects hpc
    signed_in.get("/api/list?host=locked&path=.")  # remembered login failure
    hosts = {h["alias"]: h for h in signed_in.get("/api/hosts").json()}
    assert hosts["hpc"] == {"alias": "hpc", "connected": True, "error_kind": None}
    assert hosts["server"]["connected"] is False
    assert hosts["locked"]["error_kind"] == "auth"
    assert set(hosts) == set(ALIASES)


def test_list_endpoint(signed_in):
    body = signed_in.get("/api/list", params={"host": "hpc", "path": "~/data//sub/"}).json()
    assert body["host"] == "hpc" and body["path"] == "data/sub"
    assert body["abs_path"] == "/home/tester/data/sub"
    assert [e["name"] for e in body["entries"]] == ["adir", "file.txt"]  # dirs first, "." skipped
    assert set(body["entries"][0]) == {"name", "size", "mtime", "is_dir", "is_link"}


def test_validation_errors_are_400(signed_in):
    def post(src_path="a", dst_path="b", src_host="hpc", dst_host="server"):
        body = {"src": {"host": src_host, "path": src_path}, "dst": {"host": dst_host, "path": dst_path}}
        return signed_in.post("/api/jobs", json=body)

    assert post(src_host="nope").status_code == 400
    assert post(src_host="nope").json() == {"error": "unknown_host", "message": "unknown host"}
    assert post(src_path="a\x00b").status_code == 400
    assert post(dst_path="a\nb").status_code == 400
    assert post(src_path="a" * 4097).status_code == 400
    assert post(dst_host="hpc").status_code == 400  # same host
    assert signed_in.post("/api/jobs", json={"src": 1}).status_code == 400
    assert signed_in.get("/api/list?host=nope").status_code == 400
    assert signed_in.get("/api/list", params={"host": "hpc", "path": "a\x00"}).status_code == 400
    assert signed_in.get("/api/list", params={"host": "hpc", "path": "a" * 4097}).status_code == 400
    assert post(src_path="a" * 4096).status_code == 201  # exactly at the limit


def test_job_crud_cancel_retry(signed_in):
    created = signed_in.post("/api/jobs", json={**JOB, "verify": True, "on_conflict": "overwrite"})
    assert created.status_code == 201
    job = created.json()
    assert job["options"] == {"verify": True, "links": False, "on_conflict": "overwrite"}
    assert job["src"] == {"host": "hpc", "path": "data/x"}

    assert [j["id"] for j in signed_in.get("/api/jobs").json()] == [job["id"]]
    fetched = wait_for_state(signed_in, job["id"])
    assert fetched["state"] == "failed" and fetched["message"] == "1 entries failed"
    assert {"bytes_transferred", "error_kind", "message", "state"} <= set(fetched)

    cancelled = signed_in.delete(f"/api/jobs/{job['id']}")  # finished job: a no-op
    assert cancelled.status_code == 200 and cancelled.json()["state"] == fetched["state"]


def test_retry_goes_through_and_state_conflicts_are_409(signed_in):
    job_id = signed_in.post("/api/jobs", json=JOB).json()["id"]
    wait_for_state(signed_in, job_id)
    retried = signed_in.post(f"/api/jobs/{job_id}/retry")
    assert retried.status_code == 200 and retried.json()["attempt"] == 2
    # while the retried job is queued/running or failed again, a second retry is
    # either refused with 409 or accepted (if it already failed again); never a crash
    wait_for_state(signed_in, job_id, attempt=2)
    assert signed_in.post(f"/api/jobs/{job_id}/retry").status_code == 200
    assert signed_in.post("/api/jobs", json=JOB).status_code == 201


def test_unknown_job_is_404(signed_in):
    assert signed_in.get("/api/jobs/nothere").status_code == 404
    assert signed_in.delete("/api/jobs/nothere").status_code == 404
    assert signed_in.post("/api/jobs/nothere/retry").status_code == 404


def test_error_mapping(signed_in):
    def get(host, path="."):
        return signed_in.get("/api/list", params={"host": host, "path": path})

    assert get("hpc", "missing").status_code == 404 and get("hpc", "missing").json()["error"] == "not_found"
    assert get("hpc", "missing").json()["message"] == "not found: missing on hpc"
    assert get("hpc", "denied").json()["message"] == "permission denied: denied on hpc"
    assert get("down").json()["message"].startswith("cannot reach down")
    assert get("hpc", "denied").status_code == 403 and get("hpc", "denied").json()["error"] == "permission_denied"
    for host in ("locked", "needpass"):
        response = get(host)
        assert response.status_code == 502 and response.json()["error"] == "auth"
        assert response.json()["message"].endswith(f"restart hopsync with --password-for {host}")
    assert get("badkey").status_code == 502 and get("badkey").json()["error"] == "hostkey"
    assert get("down").status_code == 502 and get("down").json()["error"] == "connection"


# ---- secrets never leak ----------------------------------------------------


def test_secrets_never_appear_in_responses_or_logs(client, caplog):
    caplog.set_level(logging.DEBUG)
    secrets_ = [PASSWORD, PASSPHRASE, TOKEN, SESSION]
    bodies = []

    def record(response, allow_cookie=False):
        text = response.text + "".join(
            f"{k}: {v}\n" for k, v in response.headers.items() if not (allow_cookie and k == "set-cookie")
        )
        bodies.append(text)
        return response

    record(client.get("/"))
    record(client.get("/auth?token=wrong"))
    record(client.get("/api/hosts"))  # 401
    record(client.get(f"/auth?token={TOKEN}", follow_redirects=False), allow_cookie=True)
    record(client.get("/api/hosts", headers={"host": "evil.example"}))
    record(client.post("/api/jobs", json=JOB, headers={"origin": "http://evil.example"}))
    for path in ("/api/hosts", "/", "/api/jobs", "/api/jobs/zzz"):
        record(client.get(path))
    for host in ALIASES:
        for p in (".", "missing", "denied"):
            record(client.get("/api/list", params={"host": host, "path": p}))
    created = record(client.post("/api/jobs", json=JOB)).json()
    record(client.post("/api/jobs", json={**JOB, "dst": {"host": "hpc", "path": "x"}}))
    record(client.post("/api/jobs", json={"bad": 1}))
    record(client.delete(f"/api/jobs/{created['id']}"))
    record(client.post(f"/api/jobs/{created['id']}/retry"))
    record(client.get("/api/jobs"))
    with client.websocket_connect("/api/events", headers=ws_headers()) as ws:
        bodies.append(str(ws.receive_json()))

    everything = "\n".join(bodies)
    # httpx (the test CLIENT) logs the URLs it requests, token included; that
    # is not our server, so its logger is left out.
    server_logs = "\n".join(r.getMessage() for r in caplog.records if not r.name.startswith(("httpx", "httpcore")))
    for secret in secrets_:
        assert secret not in everything
        assert secret not in server_logs


# ---- serving ---------------------------------------------------------------


def test_uvicorn_config_is_localhost_only_without_access_log():
    config = build_uvicorn_config(object(), 4321)
    assert config.host == "127.0.0.1" and config.port == 4321
    assert config.access_log is False
    assert str(config.log_level).lower() in ("warning", "30")


def test_there_is_no_option_to_change_the_host():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["serve", "--host", "0.0.0.0"])
    args = build_parser().parse_args(["serve", "--no-browser"])
    assert args.port is None and args.no_browser  # None: settings.toml or a free port


def test_pick_free_port():
    port = pick_free_port()
    assert 0 < port < 65536


async def test_slow_websocket_client_is_cut_off_without_blocking():
    from hopsync.api.hub import QUEUE_SIZE
    from hopsync.core.jobs import JobEntryEvent

    hub = EventHub()
    slow = hub.subscribe()
    healthy = hub.subscribe()
    for n in range(QUEUE_SIZE + 5):
        hub.publish(JobEntryEvent("j1", EntrySkipped(f"p{n}", "already copied")))
        healthy.queue.get_nowait()  # the healthy client keeps up; the slow one never reads
    assert slow.queue.qsize() == 1 and slow.queue.get_nowait() is None  # just the "close" marker
    assert slow not in hub._subscribers and healthy in hub._subscribers
