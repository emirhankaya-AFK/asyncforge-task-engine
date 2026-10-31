"""FastAPI: bounded typed inputs, hashed API key comparison, transaction-safe idempotency."""
from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from datetime import datetime, timezone
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError

from asyncforge.db import Attempt, Event, Job, session_factory
from asyncforge.handlers import validate_task

app = FastAPI(title="AsyncForge", version="0.1.0")


def _is_key_valid(provided: str, expected: str) -> bool:
    import hmac
    if not expected or len(expected) < 24:
        return False
    return hmac.compare_digest(hashlib.sha256(provided.encode()).digest(), hashlib.sha256(expected.encode()).digest())


def require_key(x_api_key: Annotated[str | None, Header()] = None):
    if not x_api_key or not _is_key_valid(x_api_key, os.environ.get("ASYNCFORGE_API_KEY", "")):
        raise HTTPException(status_code=401, detail="Unauthorized")


def require_operator(x_operator_key: Annotated[str | None, Header()] = None, _: None = Depends(require_key)):
    if not x_operator_key or not _is_key_valid(x_operator_key, os.environ.get("ASYNCFORGE_OPERATOR_KEY", "")):
        raise HTTPException(status_code=403, detail="Operator authorization required")


@app.middleware("http")
async def reject_big_body(request: Request, call_next):
    if request.method in ("POST", "PUT", "PATCH"):
        length = request.headers.get("content-length")
        if length and (not length.isdecimal() or int(length) > 8192):
            return JSONResponse({"detail": "Body too large"}, status_code=413)
        body = await request.body()
        if len(body) > 8192:
            return JSONResponse({"detail": "Body too large"}, status_code=413)
    try:
        return await call_next(request)
    except SQLAlchemyError:
        return JSONResponse({"detail": "Database unavailable"}, status_code=503)
    except RuntimeError as exc:
        if "DATABASE_URL must point to isolated PostgreSQL" in str(exc):
            return JSONResponse({"detail": "Database not configured"}, status_code=503)
        raise


class JobRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    task_type: Literal["demo.sha256", "demo.sleep", "demo.fail_n_times", "demo.permanent_fail"]
    queue: str = Field(default="default", min_length=1, max_length=64, pattern=r"^[a-z][a-z0-9_-]*$")
    payload: dict = Field(default_factory=dict)
    run_at: datetime | None = None
    priority: int = Field(default=0, ge=-10, le=10)
    max_attempts: int = Field(default=3, ge=1, le=10)
    timeout_seconds: int = Field(default=30, ge=1, le=300)

    @field_validator("run_at")
    @classmethod
    def tz_required(cls, val):
        if val is not None and (val.tzinfo is None or val.utcoffset() is None):
            raise ValueError("run_at needs timezone")
        return val.astimezone(timezone.utc) if val else val

    @field_validator("payload")
    @classmethod
    def payload_size(cls, val):
        if len(json.dumps(val, ensure_ascii=False, allow_nan=False).encode()) > 4096:
            raise ValueError("payload exceeds 4096 bytes")
        return val


def public_job(j):
    return {"id": str(j.id), "task_type": j.task_type, "queue": j.queue,
            "state": j.state, "run_at": j.run_at, "attempts": j.attempts,
            "max_attempts": j.max_attempts, "result": j.result,
            "last_error_code": j.last_error_code, "replay_of": str(j.replay_of) if j.replay_of else None,
            "created_at": j.created_at, "updated_at": j.updated_at}


def fingerprint(data: JobRequest):
    normalized = data.model_dump(mode="json", exclude={"run_at"})
    normalized["run_at"] = data.run_at.isoformat() if data.run_at else None
    return hashlib.sha256(json.dumps(normalized, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()


@app.get("/health/live")
def live():
    return {"status": "alive"}


@app.get("/health/ready")
def ready():
    try:
        with session_factory().begin() as db:
            db.execute(text("SELECT 1"))
        return {"status": "ready"}
    except Exception:
        raise HTTPException(status_code=503, detail="Database unavailable") from None


@app.post("/v1/jobs", dependencies=[Depends(require_key)])
def submit(data: JobRequest, response: Response, idempotency_key: Annotated[str | None, Header()] = None):
    if not idempotency_key or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", idempotency_key):
        raise HTTPException(status_code=422, detail="Valid Idempotency-Key required")
    try:
        validate_task(data.task_type, data.payload)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    sig = fingerprint(data)
    job_id = uuid.uuid4()
    with session_factory().begin() as db:
        stmt = (insert(Job).values(
            id=job_id, client_id="local", idempotency_key=idempotency_key, request_hash=sig,
            task_type=data.task_type, queue=data.queue, payload=data.payload,
            run_at=data.run_at if data.run_at is not None else func.now(), priority=data.priority,
            max_attempts=data.max_attempts, timeout_seconds=data.timeout_seconds,
        ).on_conflict_do_nothing(index_elements=["client_id", "idempotency_key"]).returning(Job.id))
        inserted = db.scalar(stmt)
        if inserted is not None:
            db.add(Event(job_id=job_id, event_type="submitted", details={}))
            response.status_code = 201
        else:
            response.status_code = 200
        j = db.scalar(select(Job).where(Job.client_id == "local", Job.idempotency_key == idempotency_key))
        if j.request_hash != sig:
            raise HTTPException(status_code=409, detail="Idempotency-Key content conflict")
        return public_job(j)


@app.get("/v1/jobs", dependencies=[Depends(require_key)])
def list_jobs(state: str | None = Query(default=None, pattern=r"^(queued|running|succeeded|dead|cancelled)$"),
              queue: str | None = None, page: int = Query(1, ge=1), size: int = Query(25, ge=1, le=100)):
    with session_factory().begin() as db:
        q = select(Job)
        if state:
            q = q.where(Job.state == state)
        if queue:
            q = q.where(Job.queue == queue)
        total = db.scalar(select(func.count()).select_from(q.subquery()))
        rows = db.scalars(q.order_by(Job.created_at, Job.id).offset((page - 1) * size).limit(size)).all()
        return {"page": page, "size": size, "total": total, "items": [public_job(j) for j in rows]}


@app.get("/v1/jobs/{job_id}", dependencies=[Depends(require_key)])
def get_job(job_id: uuid.UUID):
    with session_factory().begin() as db:
        j = db.get(Job, job_id)
        if not j:
            raise HTTPException(status_code=404, detail="Job not found")
        return public_job(j)


@app.get("/v1/jobs/{job_id}/attempts", dependencies=[Depends(require_key)])
def get_attempts(job_id: uuid.UUID):
    with session_factory().begin() as db:
        if not db.get(Job, job_id):
            raise HTTPException(status_code=404, detail="Job not found")
        rows = db.scalars(select(Attempt).where(Attempt.job_id == job_id).order_by(Attempt.attempt_no)).all()
        return [{"attempt_no": a.attempt_no, "generation": a.lease_generation, "worker_id": a.worker_id,
                 "outcome": a.outcome, "error_code": a.error_code, "started_at": a.started_at,
                 "finished_at": a.finished_at} for a in rows]


@app.post("/v1/jobs/{job_id}/cancel", dependencies=[Depends(require_operator)])
def cancel(job_id: uuid.UUID):
    with session_factory().begin() as db:
        j = db.scalar(select(Job).where(Job.id == job_id).with_for_update())
        if j is None:
            raise HTTPException(status_code=404, detail="Job not found")
        if j.state != "queued":
            raise HTTPException(status_code=409, detail="Only queued jobs can be cancelled")
        j.state = "cancelled"
        j.updated_at = func.now()
        db.add(Event(job_id=j.id, event_type="cancelled", details={}))
        db.flush()
        db.refresh(j)
        return public_job(j)


@app.post("/v1/jobs/{job_id}/replay", dependencies=[Depends(require_operator)])
def replay(job_id: uuid.UUID):
    with session_factory().begin() as db:
        old = db.scalar(select(Job).where(Job.id == job_id).with_for_update())
        if old is None:
            raise HTTPException(status_code=404, detail="Job not found")
        if old.state != "dead":
            raise HTTPException(status_code=409, detail="Only dead jobs can be replayed")
        new = Job(id=uuid.uuid4(), client_id=old.client_id, idempotency_key="replay:" + uuid.uuid4().hex,
                  request_hash=old.request_hash, task_type=old.task_type, queue=old.queue, payload=old.payload,
                  max_attempts=old.max_attempts, timeout_seconds=old.timeout_seconds, priority=old.priority,
                  run_at=func.now(), replay_of=old.id)
        db.add(new)
        db.flush()
        db.refresh(new)
        db.add(Event(job_id=old.id, event_type="replay_requested", details={"new_job_id": str(new.id)}))
        db.add(Event(job_id=new.id, event_type="replayed", details={"old_job_id": str(old.id)}))
        return public_job(new)
