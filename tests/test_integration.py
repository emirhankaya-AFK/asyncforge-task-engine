"""Requires separate disposable PostgreSQL and migrated schema; no mocks/SQLite."""
from __future__ import annotations

import concurrent.futures
import os
import signal
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, text
from sqlalchemy.engine import make_url

from asyncforge.api import app
from asyncforge.core import claim, complete, heartbeat
from asyncforge.db import Attempt, Job, session_factory

# PostgreSQL is mandatory: requesting integration tests without a real DB is a failure,
# never a green result made from skipped tests.
@pytest.fixture(scope="session", autouse=True)
def verify_isolated_postgres():
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        pytest.fail("DATABASE_URL is required; PostgreSQL integration cannot be skipped")
    parsed = make_url(url)
    if not parsed.drivername.startswith("postgresql") or parsed.database != "asyncforge_test":
        pytest.fail("Refusing integration tests outside the exact asyncforge_test PostgreSQL database")
    if parsed.host not in ("localhost", "127.0.0.1", "postgres-test"):
        pytest.fail("Refusing remote/production PostgreSQL host")
    try:
        with session_factory().begin() as db:
            assert db.scalar(text("SELECT current_database()")) == "asyncforge_test"
            assert db.scalar(text("SELECT to_regclass('public.jobs')")) == "jobs"
    except Exception as exc:
        pytest.fail(f"PostgreSQL is unavailable or migration was not applied: {type(exc).__name__}")



def headers(key=None, operator=False):
    value = {"X-API-Key": key or os.environ["ASYNCFORGE_API_KEY"]}
    if operator:
        value["X-Operator-Key"] = os.environ["ASYNCFORGE_OPERATOR_KEY"]
    return value


def payload(task="demo.sha256", data=None, **kwargs):
    return {"task_type": task, "payload": data if data is not None else {"text": "hello"}, **kwargs}


def submit(client, key, data):
    return client.post("/v1/jobs", headers={**headers(), "Idempotency-Key": key}, json=data)


@pytest.fixture(autouse=True)
def clean_database():
    with session_factory().begin() as db:
        # isolated test database only; avoid truncating live databases
        assert make_url(os.environ["DATABASE_URL"]).database == "asyncforge_test"
        db.execute(text("TRUNCATE TABLE job_events, job_attempts, jobs RESTART IDENTITY CASCADE"))
    yield


def wait_state(client, ids, timeout=25):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with session_factory().begin() as db:
            states = {str(row.id): row.state for row in db.scalars(select(Job).where(Job.id.in_([uuid.UUID(x) for x in ids])))}
        if all(states.get(x) in ("succeeded", "dead") for x in ids):
            return states
        time.sleep(0.1)
    raise AssertionError("Job(s) did not become terminal in bounded test")


def start_worker(worker_id, max_idle=4):
    return subprocess.Popen([sys.executable, "-m", "asyncforge.worker", "--worker-id", worker_id,
                             "--max-idle", str(max_idle)], env=os.environ.copy(),
                            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)


def stop_workers(workers):
    for p in workers:
        if p.poll() is None:
            p.terminate()
    for p in workers:
        try:
            p.communicate(timeout=12)
        except subprocess.TimeoutExpired:
            p.kill()
            p.communicate()


def test_auth_validation_idempotency_schedule_and_replay():
    with TestClient(app) as client:
        data = payload()
        assert client.post("/v1/jobs", json=data).status_code == 401
        assert client.post("/v1/jobs", headers={"X-API-Key": "invalid"}, json=data).status_code == 401
        assert submit(client, "bad-schema", payload(task="demo.sha256", data={"python": "print(1)"})).status_code == 422
        assert submit(client, "oversize", payload(data={"text": "x" * 5000})).status_code == 422
        assert client.post("/v1/jobs", headers={**headers(), "Idempotency-Key": "oversize-body"},
                           content=b"x" * 9000).status_code == 413
        r = submit(client, "same", data)
        assert r.status_code == 201
        job_id = r.json()["id"]
        assert submit(client, "same", data).json()["id"] == job_id
        assert submit(client, "same", payload(data={"text": "different"})).status_code == 409
        assert client.get("/health/ready").status_code == 200
        assert client.get(f"/v1/jobs/{job_id}", headers=headers()).status_code == 200
        assert client.get("/v1/jobs?page=1&size=1", headers=headers()).json()["total"] == 1
        future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        j = submit(client, "future", payload(run_at=future)).json()["id"]
        assert claim("scheduler-check") is not None  # ready job
        assert claim("scheduler-check") is None  # scheduled job cannot be claimed
        assert client.post(f"/v1/jobs/{j}/cancel", headers=headers()).status_code == 403
        assert client.post(f"/v1/jobs/{j}/cancel", headers=headers(operator=True)).status_code == 200
        dead = submit(client, "dead", payload("demo.permanent_fail", data={})).json()["id"]
        c = claim("manual", lease_seconds=3)
        assert c and str(c["id"]) == dead
        assert complete(c["id"], "manual", c["generation"], error_code="PERMANENT_TASK_ERROR", retryable=False)
        assert client.post(f"/v1/jobs/{dead}/replay", headers=headers()).status_code == 403
        replay = client.post(f"/v1/jobs/{dead}/replay", headers=headers(operator=True))
        assert replay.status_code == 200
        assert replay.json()["id"] != dead and replay.json()["replay_of"] == dead
        assert client.get(f"/v1/jobs/{dead}/attempts", headers=headers()).json()[0]["outcome"] == "dead"


def test_parallel_same_idempotency():
    def one(_):
        with TestClient(app) as c:
            r = submit(c, "parallel", payload())
            return r.status_code, r.json()["id"]
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        rows = list(pool.map(one, range(32)))
    assert len(set(x[1] for x in rows)) == 1
    assert sum(x[0] == 201 for x in rows) == 1
    with TestClient(app) as c:
        assert submit(c, "parallel", payload(data={"text": "conflicting"})).status_code == 409
    with session_factory().begin() as db:
        assert db.scalar(select(Job.id).where(Job.idempotency_key == "parallel")) == uuid.UUID(rows[0][1])


def test_fencing_old_generation_denied():
    with TestClient(app) as client:
        j = submit(client, "fencing", payload()).json()["id"]
    a = claim("old", lease_seconds=1)
    assert str(a["id"]) == j
    time.sleep(1.3)
    b = claim("new", lease_seconds=4)
    assert b and b["id"] == a["id"] and b["generation"] > a["generation"]
    assert not heartbeat(a["id"], "old", a["generation"])
    assert not complete(a["id"], "old", a["generation"], result={"incorrect": True})
    assert complete(b["id"], "new", b["generation"], result={"correct": True})
    with session_factory().begin() as db:
        assert db.get(Job, b["id"]).result == {"correct": True}


def test_four_independent_workers_and_retries_dlq():
    with TestClient(app) as client:
        ids = [submit(client, f"group-{i}", payload(task="demo.sleep", data={"seconds": .12})).json()["id"] for i in range(24)]
        fail = submit(client, "retry", payload("demo.fail_n_times", data={"failures": 9}, max_attempts=2)).json()["id"]
        succeed_retry = submit(client, "recover", payload("demo.fail_n_times", data={"failures": 1}, max_attempts=3)).json()["id"]
    workers = [start_worker(f"worker-{i}", 3) for i in range(4)]
    try:
        states = wait_state(None, ids + [fail, succeed_retry], 35)
        assert all(states[x] == "succeeded" for x in ids)
        assert states[fail] == "dead"
        assert states[succeed_retry] == "succeeded"
        with session_factory().begin() as db:
            assigned = db.scalars(select(Attempt.worker_id).where(Attempt.job_id.in_([uuid.UUID(x) for x in ids]))).all()
            assert len(set(assigned)) == 4, f"Expected all 4 independent workers to claim jobs: {set(assigned)}"
            assert db.get(Job, uuid.UUID(ids[0])).result == {"slept": .12}
            assert len(db.scalars(select(Attempt).where(Attempt.job_id == uuid.UUID(fail))).all()) == 2
    finally:
        stop_workers(workers)


def test_sigkill_reclaim_and_graceful_sigterm():
    with TestClient(app) as client:
        j = submit(client, "crash", payload("demo.sleep", data={"seconds": 4}, max_attempts=2, timeout_seconds=8)).json()["id"]
    p = start_worker("victim", 6)
    try:
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            with session_factory().begin() as db:
                obj = db.get(Job, uuid.UUID(j))
                if obj.state == "running":
                    break
            time.sleep(.05)
        else:
            pytest.fail("worker never claimed")
        p.kill()  # child may remain; must not change DB result
        p.communicate(timeout=5)
        with session_factory().begin() as db:
            obj = db.get(Job, uuid.UUID(j))
            assert obj.state == "running"
        time.sleep(7)  # default lease 6 seconds
        recovery = start_worker("recovery", 7)
        try:
            assert wait_state(None, [j], 20)[j] == "succeeded"
            with session_factory().begin() as db:
                assert db.get(Job, uuid.UUID(j)).attempts == 2
        finally:
            stop_workers([recovery])
    finally:
        stop_workers([p])
    with TestClient(app) as client:
        j2 = submit(client, "graceful", payload("demo.sleep", data={"seconds": 2})).json()["id"]
    p2 = start_worker("graceful", 8)
    try:
        # SIGTERM only after this worker has claimed the task, so we test drain
        # rather than racing a worker that never accepted a job.
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            with session_factory().begin() as db:
                observed = db.get(Job, uuid.UUID(j2))
                if observed.state == "running" and observed.lease_owner == "graceful":
                    break
            time.sleep(.05)
        else:
            pytest.fail("graceful worker did not claim job")
        p2.send_signal(signal.SIGTERM)
        assert wait_state(None, [j2], 12)[j2] == "succeeded"
        p2.wait(timeout=8)
        assert p2.returncode == 0
    finally:
        stop_workers([p2])


def test_heartbeat_deadline_prevents_infinite_extension():
    with TestClient(app) as client:
        j = submit(client, "timeout-lease", payload("demo.sleep", data={"seconds": 5}, timeout_seconds=1)).json()["id"]
    task = claim("deadline-owner", lease_seconds=3)
    assert str(task["id"]) == j
    time.sleep(1.35)
    assert not heartbeat(task["id"], "deadline-owner", task["generation"], lease_seconds=3)
    assert not complete(task["id"], "deadline-owner", task["generation"], result={"too_late": True})


def test_worker_enforces_execution_timeout():
    """An actual child is terminated; heartbeat cannot extend it indefinitely."""
    with TestClient(app) as client:
        j = submit(client, "hard-timeout", payload("demo.sleep", data={"seconds": 5},
                                                   timeout_seconds=1, max_attempts=1)).json()["id"]
    p = start_worker("timeout-worker", 3)
    try:
        assert wait_state(None, [j], 12)[j] == "dead"
        with session_factory().begin() as db:
            row = db.get(Job, uuid.UUID(j))
            assert row.last_error_code == "TASK_TIMEOUT"
            assert row.attempts == 1
    finally:
        stop_workers([p])


def test_positive_heartbeat_and_deadline_fencing():
    """Current owner can renew, and wrong generation cannot, even before expiry."""
    with TestClient(app) as client:
        j = submit(client, "heartbeat-positive", payload("demo.sleep", data={"seconds": 0},
                                                       timeout_seconds=8)).json()["id"]
    record = claim("heartbeat-owner", lease_seconds=2)
    assert str(record["id"]) == j
    with session_factory().begin() as db:
        old_expiry = db.get(Job, record["id"]).lease_until
    time.sleep(.25)
    assert heartbeat(record["id"], "heartbeat-owner", record["generation"], lease_seconds=4)
    assert not heartbeat(record["id"], "heartbeat-owner", record["generation"] + 1)
    with session_factory().begin() as db:
        assert db.get(Job, record["id"]).lease_until > old_expiry
    assert complete(record["id"], "heartbeat-owner", record["generation"], result={"slept": 0})
