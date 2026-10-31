"""Database mappings. PostgreSQL server time drives all scheduling and leases."""
from __future__ import annotations

import os
import uuid
from datetime import datetime

from sqlalchemy import (
    CheckConstraint, DateTime, ForeignKey, Index, Integer, String, UniqueConstraint,
    create_engine, func, text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


class Base(DeclarativeBase):
    pass


class Job(Base):
    __tablename__ = "jobs"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    client_id: Mapped[str] = mapped_column(String(64), nullable=False, default="local")
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    task_type: Mapped[str] = mapped_column(String(80), nullable=False)
    queue: Mapped[str] = mapped_column(String(64), nullable=False, default="default")
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    state: Mapped[str] = mapped_column(String(20), nullable=False, default="queued")
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    run_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    timeout_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=30)
    lease_owner: Mapped[str | None] = mapped_column(String(128))
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_generation: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    deadline_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    result: Mapped[dict | None] = mapped_column(JSONB)
    last_error_code: Mapped[str | None] = mapped_column(String(80))
    replay_of: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("jobs.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
    __table_args__ = (
        UniqueConstraint("client_id", "idempotency_key", name="uq_job_idempotency"),
        CheckConstraint("attempts >= 0 AND max_attempts BETWEEN 1 AND 10", name="ck_attempts"),
        CheckConstraint("timeout_seconds BETWEEN 1 AND 300", name="ck_timeout"),
        CheckConstraint("state IN ('queued','running','succeeded','dead','cancelled')", name="ck_state"),
        Index("ix_jobs_ready", text("priority DESC"), "run_at", "created_at", postgresql_where=text("state='queued'")),
        Index("ix_jobs_expired", "lease_until", postgresql_where=text("state='running'")),
    )


class Attempt(Base):
    __tablename__ = "job_attempts"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("jobs.id"), nullable=False)
    attempt_no: Mapped[int] = mapped_column(Integer, nullable=False)
    lease_generation: Mapped[int] = mapped_column(Integer, nullable=False)
    worker_id: Mapped[str] = mapped_column(String(128), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    outcome: Mapped[str] = mapped_column(String(32), nullable=False, default="running")
    error_code: Mapped[str | None] = mapped_column(String(80))
    __table_args__ = (UniqueConstraint("job_id", "attempt_no"),)


class Event(Base):
    __tablename__ = "job_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("jobs.id"), nullable=False)
    event_type: Mapped[str] = mapped_column(String(48), nullable=False)
    details: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


def new_engine():
    url = os.environ.get("DATABASE_URL")
    if not url or not url.startswith(("postgresql+psycopg://", "postgresql://")):
        raise RuntimeError("DATABASE_URL must point to isolated PostgreSQL")
    return create_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=5)


engine = None
Session = None


def session_factory():
    global engine, Session
    if Session is None:
        engine = new_engine()
        Session = sessionmaker(engine, expire_on_commit=False)
    return Session
