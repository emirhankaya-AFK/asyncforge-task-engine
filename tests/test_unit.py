import hashlib

import pytest
from pydantic import ValidationError

from asyncforge.api import JobRequest, _is_key_valid, fingerprint
from asyncforge.handlers import PermanentTaskError, RetryableTaskError, execute, validate_task


def test_handlers_allowlist():
    assert execute("demo.sha256", {"text": "hello"}, 1)["sha256"] == hashlib.sha256(b"hello").hexdigest()
    assert execute("demo.fail_n_times", {"failures": 1}, 2) == {"completed_after": 2}
    with pytest.raises(RetryableTaskError):
        execute("demo.fail_n_times", {"failures": 1}, 1)
    with pytest.raises(PermanentTaskError):
        execute("demo.permanent_fail", {}, 1)
    for task, payload in [("shell", {"code": "rm -rf /"}), ("demo.sleep", {"seconds": 900}),
                          ("demo.sha256", {"command": "echo hi"}), ("demo.permanent_fail", {"extra": 1})]:
        with pytest.raises(ValueError):
            validate_task(task, payload)


def test_schema_boundaries():
    with pytest.raises(ValidationError):
        JobRequest(task_type="demo.sha256", payload={"text": "x"}, extra="forbidden")
    with pytest.raises(ValidationError):
        JobRequest(task_type="demo.sha256", payload={"text": "x"}, run_at="2040-01-01T00:00:00")
    with pytest.raises(ValidationError):
        JobRequest(task_type="demo.sha256", payload={"text": "x"}, max_attempts=0)
    with pytest.raises(ValidationError):
        JobRequest(task_type="demo.sha256", payload={"text": "x" * 5000})
    assert fingerprint(JobRequest(task_type="demo.sha256", payload={"text": "x"})) == fingerprint(
        JobRequest(task_type="demo.sha256", payload={"text": "x"}))


def test_key_verifier():
    assert _is_key_valid("a" * 24, "a" * 24)
    assert not _is_key_valid("a" * 24, "b" * 24)
    assert not _is_key_valid("a" * 24, "")


def test_api_rejects_missing_or_bad_api_key_without_database(monkeypatch):
    from fastapi.testclient import TestClient
    from asyncforge.api import app

    monkeypatch.setenv("ASYNCFORGE_API_KEY", "a" * 32)
    with TestClient(app) as client:
        assert client.get("/health/live").json() == {"status": "alive"}
        assert client.get("/v1/jobs").status_code == 401
        assert client.get("/v1/jobs", headers={"X-API-Key": "wrong"}).status_code == 401
        assert client.post("/v1/jobs", headers={"X-API-Key": "a" * 32},
                           json={"task_type": "unknown", "payload": {}}).status_code == 422
        assert client.post("/v1/jobs", headers={"X-API-Key": "a" * 32},
                           content=b"x" * 9000).status_code == 413


def test_unconfigured_database_fails_closed(monkeypatch):
    from fastapi.testclient import TestClient
    import asyncforge.db as db
    from asyncforge.api import app

    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("ASYNCFORGE_API_KEY", "a" * 32)
    monkeypatch.setattr(db, "Session", None)
    with TestClient(app) as client:
        r = client.post("/v1/jobs", headers={"X-API-Key": "a" * 32, "Idempotency-Key": "db-unconfigured"},
                        json={"task_type": "demo.sha256", "payload": {"text": "abc"}})
        assert r.status_code == 503
        assert client.get("/health/ready").status_code == 503
