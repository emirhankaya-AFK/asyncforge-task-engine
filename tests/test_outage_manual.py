"""Destructive outage test: ONLY explicitly named disposable PostgreSQL test container."""
from __future__ import annotations

import os
import subprocess
import time

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.engine import make_url

from asyncforge.api import app


def docker(*args):
    return subprocess.run(["docker", *args], check=True, capture_output=True, text=True, timeout=45).stdout


def test_real_postgres_stop_and_recovery():
    url = os.environ.get("DATABASE_URL", "")
    if not url or make_url(url).database != "asyncforge_test" or make_url(url).host not in ("localhost", "127.0.0.1"):
        pytest.fail("Outage test requires isolated local asyncforge_test Postgres URL")
    cid = os.environ.get("ASYNCFORGE_TEST_CONTAINER_ID", "")
    if not cid or len(cid) < 12 or any(x not in "0123456789abcdef" for x in cid.lower()):
        pytest.fail("Explicit disposable PostgreSQL Docker container ID required")
    try:
        image = docker("inspect", "--format", "{{.Config.Image}}", cid).strip()
        env = docker("inspect", "--format", "{{range .Config.Env}}{{println .}}{{end}}", cid)
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        pytest.fail(f"Docker unavailable or specified test container inaccessible: {type(exc).__name__}")
    if not image.startswith("postgres:16") or "POSTGRES_DB=asyncforge_test" not in env.splitlines():
        pytest.fail("Refusing to stop any container except isolated postgres:16 asyncforge_test")

    with TestClient(app) as c:
        assert c.get("/health/ready").status_code == 200
        try:
            docker("stop", cid)
            deadline = time.monotonic() + 12
            while time.monotonic() < deadline:
                if c.get("/health/ready").status_code == 503:
                    break
                time.sleep(.2)
            else:
                pytest.fail("Readiness never failed when test PostgreSQL stopped")
            assert c.get("/v1/jobs", headers={"X-API-Key": os.environ["ASYNCFORGE_API_KEY"]}).status_code == 503
        finally:
            docker("start", cid)
        deadline = time.monotonic() + 35
        while time.monotonic() < deadline:
            if c.get("/health/ready").status_code == 200:
                break
            time.sleep(.3)
        else:
            pytest.fail("PostgreSQL did not recover within 35 seconds")
        assert c.get("/v1/jobs", headers={"X-API-Key": os.environ["ASYNCFORGE_API_KEY"]}).status_code == 200
