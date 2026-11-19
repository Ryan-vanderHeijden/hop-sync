"""The conflict endpoint and on_conflict handling in the API."""

import time

import pytest
from fastapi.testclient import TestClient
from test_api_unit import ALIASES, JOB, ORIGIN, PORT, SESSION, TOKEN, fake_connector, fake_resolver, ws_headers

from hopsync.api.app import create_app
from hopsync.api.hub import EventHub
from hopsync.core.conflict import ConflictInfo
from hopsync.core.jobs import JobManager
from hopsync.core.sessions import SessionPool

decisions = []


async def conflicting_copier(src, src_path, dst, dst_path, on_event, *, on_conflict, **kw):
    for name in ("a.txt", "b.txt"):
        decisions.append(await on_conflict(ConflictInfo(f"/out/{name}", False, 10, 1, 20, 2)))


@pytest.fixture
def client():
    decisions.clear()
    pool = SessionPool(connector=fake_connector)
    hub = EventHub()
    manager = JobManager(pool, hub.publish, copier=conflicting_copier, resolver=fake_resolver)
    app = create_app(manager, pool, ALIASES, TOKEN, SESSION, PORT, hub)
    with TestClient(app, base_url=ORIGIN) as test_client:
        test_client.get(f"/auth?token={TOKEN}", follow_redirects=False)
        yield test_client


def wait_for(predicate):
    for _ in range(300):
        value = predicate()
        if value:
            return value
        time.sleep(0.01)
    raise AssertionError("timed out")


def pending_of(client, job_id):
    return client.get(f"/api/jobs/{job_id}").json()["pending_conflict"]


def test_ask_is_the_default_and_the_conflict_is_answered_over_the_api(client):
    job = client.post("/api/jobs", json=JOB).json()
    assert job["options"]["on_conflict"] == "ask"
    first = wait_for(lambda: pending_of(client, job["id"]))
    assert first["path"] == "/out/a.txt" and first["src_size"] == 10 and first["dst_size"] == 20

    # a client that connects later sees the pending conflict in its snapshot
    with client.websocket_connect("/api/events", headers=ws_headers()) as ws:
        snapshot = ws.receive_json()
        assert snapshot["jobs"][0]["pending_conflict"]["conflict_id"] == first["conflict_id"]

    wrong = client.post(f"/api/jobs/{job['id']}/conflict",
                        json={"conflict_id": "nope", "decision": "skip", "apply_to_all": False})
    assert wrong.status_code == 409
    bad_decision = client.post(f"/api/jobs/{job['id']}/conflict",
                               json={"conflict_id": first["conflict_id"], "decision": "explode"})
    assert bad_decision.status_code == 400

    ok = client.post(f"/api/jobs/{job['id']}/conflict",
                     json={"conflict_id": first["conflict_id"], "decision": "skip", "apply_to_all": True})
    assert ok.status_code == 200
    wait_for(lambda: client.get(f"/api/jobs/{job['id']}").json()["state"] == "done")
    assert decisions == ["skip", "skip"]

    # answering again, or for an unknown job, is refused
    again = client.post(f"/api/jobs/{job['id']}/conflict",
                        json={"conflict_id": first["conflict_id"], "decision": "skip"})
    assert again.status_code == 409
    assert client.post("/api/jobs/zzz/conflict", json={"conflict_id": "x", "decision": "skip"}).status_code == 404


def test_other_policies_are_accepted_and_bad_ones_rejected(client):
    for policy in ("refuse", "ask", "skip", "rename", "overwrite"):
        assert client.post("/api/jobs", json={**JOB, "on_conflict": policy}).status_code == 201
    assert client.post("/api/jobs", json={**JOB, "on_conflict": "maybe"}).status_code == 400
