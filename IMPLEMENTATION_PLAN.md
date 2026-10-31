# AsyncForge — Implementation Plan and Acceptance Criteria

**MVP objective:** Durable PostgreSQL-backed multi-worker task execution with scheduling, idempotency, retries, lease recovery, failures and inspectable outcomes. Local only and free/open-source.

## Phase 0 — New repository contract (no existing app modified)

- Confirm available repo name `asyncforge-task-engine` under `emirhankaya-AFK`. New standalone **PRIVATE** GitHub repository only; owner approved private creation. Never change visibility, and never expose existing secrets.
- Create MIT LICENSE, English README with realistic claims, `SECURITY.md`, architecture decision record `docs/adr/0001-postgres-queue.md`, `.env.example`, ignore files and `pyproject.toml` pinned dependencies.
- Define semver and `MVP != production validated` badges/documentation.

**Exit:** fresh clone installs and tests locally; no outbound calls, no secrets.

## Phase 1 — DB and API

- Docker Compose: `postgres`, `api`; migration command via Alembic.
- Model jobs, attempts and events; constraints and indexes.
- Implement `POST /v1/jobs` with allowlisted task types, strict request schemas, scoped API key and transaction-safe `Idempotency-Key`.
- Add GET/list/details, health and readiness; migrations from empty DB.

**Exit:** restart PostgreSQL/API without losing existing jobs; duplicate request gives same ID; changed payload with duplicate key returns `409`.

## Phase 2 — Concurrent workers and leases

- Implement atomic `FOR UPDATE SKIP LOCKED` claim-and-update; no database transaction held during task processing.
- Worker heartbeat, lease generation fencing, graceful shutdown; lease expiry reclaim and reaper.
- Demo task handlers and safe result persistence.
- Use 2–4 local worker processes; no scheduler service needed for due jobs.

**Exit:** no concurrently valid ownership of same job when multiple workers race; crash recovery and stale-ack rejection tests pass.

## Phase 3 — Retry, backoff, dead jobs, controls

- Retryable vs non-retryable errors. Exponential backoff + jitter; max attempts.
- `dead` list, attempt history, explicit manual replay to a new job, auditable cancel for queued tasks.
- Future `run_at` scheduling, priority ordering within available jobs, bounded queue polling and DB-down behavior.

**Exit:** transient failure is retried; poison jobs become dead; scheduled jobs don't execute early; DB outage never reports task success.

## Phase 4 — Observability and portfolio proof

- Structured logs with `job_id`, `attempt_id`, `worker_id`, `lease_generation`, `trace_id` where available; no sensitive payloads.
- Metrics: `jobs_submitted_total`, `jobs_succeeded_total`, `jobs_dead_total`, `job_retries_total`, `job_lease_reclaims_total`, `job_queue_wait_seconds`, `job_duration_seconds`, `workers_active`.
- CLI commands: `asyncforge status`, `asyncforge jobs`, `asyncforge demo submit --count 1000`.
- Save reproducible load benchmark with machine/worker counts, throughput and P50/P95 latency. Do not preclaim target throughput.
- CI optional after runner/billing check; all acceptance gates must run locally first.

**Exit:** report includes actual measured numbers and machine details; screenshot/demo and failure-injection proof; docs clearly distinguish local passing tests from real deployment.

## Required test matrix

| ID | Test | Expected proof |
|---|---|---|
| T01 | Create and read job | Persistent UUID/status in DB |
| T02 | Duplicate `Idempotency-Key` + same payload | Same job ID; one DB record |
| T03 | Duplicate key + different payload | 409 Conflict |
| T04 | Parallel claim by 4 workers | One current lease owner per job |
| T05 | Kill worker mid-task | Expired lease reclaimed; no permanent stuck job |
| T06 | Old worker reports result after reclaim | Fenced; cannot overwrite latest state |
| T07 | Transient errors | Backoff and bounded attempts |
| T08 | Permanent errors | Terminal dead state and attempt history |
| T09 | Delayed `run_at` | Never claimed before due time |
| T10 | API key invalid/missing | 401/403, no mutation |
| T11 | PostgreSQL unavailable | Explicit readiness failure and no in-memory fallback |
| T12 | Unsupported task type / oversized payload | Rejected safely |
| T13 | Cancel queued / cannot cancel running | Correct status and audit event |
| T14 | Replay a dead job | New ID + auditable lineage |
| T15 | 1000 synthetic tasks, multiple workers | Recorded terminal outcomes, DB reconciliation, throughput and latency; no unsupported success claim |

## Proposed source layout

```text
asyncforge-task-engine/
  src/asyncforge/
    api/{main.py,routes_jobs.py,auth.py,schemas.py}
    core/{config.py,errors.py,registry.py}
    db/{models.py,session.py,claims.py}
    worker/{runner.py,lease.py,reaper.py,retry.py}
    tasks/{demo.py}
    observability/{metrics.py,logging.py}
    cli.py
  migrations/versions/
  tests/{unit,integration,chaos}/
  benchmarks/
  docs/adr/
  compose.yaml
  Dockerfile
  pyproject.toml
  .env.example
  README.md
  ARCHITECTURE.md
  IMPLEMENTATION_PLAN.md
```

## Budget and operations

- Local Windows + Docker Compose + PostgreSQL + Python: zero paid services (Docker Desktop license eligibility should be verified for the user's use; Podman is an open-source alternative).
- GitHub repo PRIVATE: owner approved this only; private Actions runner minutes may be billable, so verify budget before enabling automatic runs.
- Avoid paid Render compute, always-on cloud worker, commercial Redis, external AI APIs and billable GitHub Actions. Public GitHub Actions CI may be available without charges on standard runners, but do not depend on that to pass acceptance.
- A desktop that is off will not run workers; 24/7 production uptime cannot be claimed with a local-only MVP.

## MVP done definition

1. **Code:** migrations, API, worker, replay, tests and docs exist.
2. **Local verification:** repeatable test run and crash-injection test, evidence timestamp and environment.
3. **Scale demonstration:** reproducible 1000-job synthetic benchmark with numbers measured, not guessed.
4. **Security:** no arbitrary task execution; no secrets in git/logs; no production integrations.
5. **Honesty:** `MVP ready locally` only when observed; `production ready` requires independent deployed verification, backups, security hardening and live monitoring.
