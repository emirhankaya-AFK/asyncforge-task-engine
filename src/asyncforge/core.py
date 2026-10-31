"""Transactional claim, fencing, deadline, retry and reconciliation routines."""
from __future__ import annotations

import random
from datetime import timedelta

from sqlalchemy import and_, func, or_, select, update

from asyncforge.db import Attempt, Event, Job, session_factory

LEASE_SECONDS = 6  # short local test/demo lease; configurable without depending on worker heartbeats


def event(db, job, kind, details=None):
    db.add(Event(job_id=job.id, event_type=kind, details=details or {}))


def reclaim_or_dead(db, j):
    """Current expired owner can never publish; every re-claim increments fencing token."""
    previous = db.scalar(select(Attempt).where(Attempt.job_id == j.id, Attempt.attempt_no == j.attempts))
    if previous and previous.outcome == "running":
        previous.outcome = "lease_expired"
        previous.finished_at = func.now()
        previous.error_code = "LEASE_EXPIRED"
    event(db, j, "lease_expired", {"generation": j.lease_generation})
    if j.attempts >= j.max_attempts:
        j.state = "dead"
        j.lease_owner = None
        j.lease_until = None
        j.last_error_code = "LEASE_EXPIRED"
        event(db, j, "dead", {"reason": "max_attempts"})
        return False
    return True


def claim(worker_id: str, queue="default", lease_seconds=LEASE_SECONDS):
    Session = session_factory()
    with Session.begin() as db:
        # Drain expired exhausted leases separately; otherwise one poison job can starve ready jobs.
        expired = db.scalars(
            select(Job).where(Job.state == "running", Job.lease_until <= func.now(), Job.attempts >= Job.max_attempts)
            .order_by(Job.lease_until).with_for_update(skip_locked=True).limit(10)
        ).all()
        for old in expired:
            reclaim_or_dead(db, old)
        j = db.scalar(
            select(Job).where(
                Job.queue == queue,
                or_(and_(Job.state == "queued", Job.run_at <= func.now()),
                    and_(Job.state == "running", Job.lease_until <= func.now(), Job.attempts < Job.max_attempts)),
            ).order_by(Job.priority.desc(), Job.run_at, Job.created_at)
            .with_for_update(skip_locked=True).limit(1)
        )
        if j is None:
            return None
        if j.state == "running":
            reclaim_or_dead(db, j)
        j.state = "running"
        j.attempts += 1
        j.lease_generation += 1
        j.lease_owner = worker_id
        j.lease_until = func.now() + timedelta(seconds=lease_seconds)
        j.deadline_at = func.now() + timedelta(seconds=j.timeout_seconds)
        j.updated_at = func.now()
        a = Attempt(job_id=j.id, attempt_no=j.attempts, lease_generation=j.lease_generation, worker_id=worker_id)
        db.add(a)
        event(db, j, "claimed", {"attempt": j.attempts, "generation": j.lease_generation})
        db.flush()
        return {"id": j.id, "task_type": j.task_type, "payload": j.payload,
                "attempt": j.attempts, "generation": j.lease_generation, "timeout": j.timeout_seconds}


def owned(job_id, worker_id, generation):
    return and_(Job.id == job_id, Job.state == "running", Job.lease_owner == worker_id,
                Job.lease_generation == generation, Job.lease_until > func.now(), Job.deadline_at > func.now())


def heartbeat(job_id, worker_id, generation, lease_seconds=LEASE_SECONDS):
    with session_factory().begin() as db:
        result = db.execute(update(Job).where(owned(job_id, worker_id, generation)).values(
            lease_until=func.least(func.now() + timedelta(seconds=lease_seconds), Job.deadline_at),
            updated_at=func.now()))
        return result.rowcount == 1


def complete(job_id, worker_id, generation, result=None, error_code=None, retryable=False):
    """Compare-and-set with SELECT FOR UPDATE; late completions cannot mutate newer attempt."""
    with session_factory().begin() as db:
        j = db.scalar(select(Job).where(Job.id == job_id).with_for_update())
        if j is None or j.state != "running" or j.lease_owner != worker_id or j.lease_generation != generation:
            return False
        # Database clock is authoritative; lease/deadline expiration refuses late success.
        valid = db.scalar(select(Job.id).where(owned(job_id, worker_id, generation)))
        if valid is None:
            return False
        a = db.scalar(select(Attempt).where(Attempt.job_id == job_id, Attempt.attempt_no == j.attempts))
        if error_code is None:
            j.state = "succeeded"
            j.result = result
            a.outcome = "succeeded"
            event(db, j, "succeeded")
        else:
            j.last_error_code = error_code
            a.error_code = error_code
            if retryable and j.attempts < j.max_attempts:
                j.state = "queued"
                delay = min(20.0, 0.5 * (2 ** min(j.attempts - 1, 8))) + random.uniform(0, 0.2)
                j.run_at = func.now() + timedelta(seconds=delay)
                a.outcome = "retry"
                event(db, j, "retry_scheduled", {"backoff_ms": round(delay * 1000)})
            else:
                j.state = "dead"
                a.outcome = "dead"
                event(db, j, "dead", {"reason": error_code})
        a.finished_at = func.now()
        j.lease_owner = None
        j.lease_until = None
        j.deadline_at = None
        j.updated_at = func.now()
        return True


def timeout_reap():
    """Timeout is hard-bounded by worker subprocess; stale leases are terminal after exhaustion."""
    with session_factory().begin() as db:
        rows = db.scalars(select(Job).where(
            Job.state == "running", Job.lease_until <= func.now(), Job.attempts >= Job.max_attempts,
        ).with_for_update(skip_locked=True).limit(30)).all()
        for row in rows:
            reclaim_or_dead(db, row)
        return len(rows)
