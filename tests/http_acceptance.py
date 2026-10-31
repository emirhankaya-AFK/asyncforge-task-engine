"""Real HTTP + PostgreSQL + independent worker smoke acceptance (no mocks)."""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time

import httpx
from sqlalchemy.engine import make_url


def main():
    url = os.environ.get("DATABASE_URL", "")
    if not url or make_url(url).database != "asyncforge_test" or make_url(url).host not in ("localhost", "127.0.0.1"):
        raise SystemExit("Refusing HTTP acceptance outside local asyncforge_test PostgreSQL")
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    api = subprocess.Popen([sys.executable, "-m", "uvicorn", "asyncforge.api:app", "--host", "127.0.0.1",
                            "--port", str(port), "--no-access-log"], stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL)
    worker = None
    try:
        with httpx.Client(base_url=base, timeout=3) as c:
            deadline = time.monotonic() + 12
            while time.monotonic() < deadline:
                if api.poll() is not None:
                    raise RuntimeError("API process exited before readiness")
                try:
                    if c.get("/health/live").status_code == 200:
                        break
                except httpx.ConnectError:
                    pass
                time.sleep(.1)
            else:
                raise RuntimeError("HTTP startup timeout")
            assert c.get("/health/ready").status_code == 200
            assert c.get("/v1/jobs").status_code == 401
            key = os.environ["ASYNCFORGE_API_KEY"]
            headers = {"X-API-Key": key, "Idempotency-Key": "real-http-acceptance"}
            body = {"task_type": "demo.sha256", "payload": {"text": "real-http-acceptance"}}
            response = c.post("/v1/jobs", headers=headers, json=body)
            assert response.status_code == 201, response.text
            job_id = response.json()["id"]
            assert c.post("/v1/jobs", headers=headers, json=body).status_code == 200
            assert c.post("/v1/jobs", headers=headers, json={**body,"payload":{"text":"changed"}}).status_code == 409
            worker = subprocess.Popen([sys.executable, "-m", "asyncforge.worker", "--worker-id", "http-acceptance",
                                       "--max-idle", "2"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                result = c.get(f"/v1/jobs/{job_id}", headers={"X-API-Key": key})
                assert result.status_code == 200, result.text
                if result.json()["state"] == "succeeded":
                    assert len(result.json()["result"]["sha256"]) == 64
                    break
                time.sleep(.2)
            else:
                raise AssertionError("Real HTTP job did not reach succeeded")
        print(json.dumps({"http_live": 200, "http_ready": 200, "unauthorized": 401,
                          "create": 201, "same_idempotency": 200, "conflict": 409,
                          "job_id": job_id, "terminal_state": "succeeded"}, indent=2))
    finally:
        for proc in (worker, api):
            if proc is None:
                continue
            if proc.poll() is None:
                proc.terminate()
            try:
                proc.wait(timeout=12)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


if __name__ == "__main__":
    main()
