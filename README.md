# AsyncForge Task Engine

A **local-first, PostgreSQL-backed, at-least-once Python task engine**. This is a production-shaped MVP, **not a production SLA**. No Redis, Kubernetes, LLM API, customer data or external side effects. Workers run *only* four allowlisted demo handlers.

## Features

- FastAPI create/read/paged-list/schedule; strict typed payloads (4 KiB payload, 8 KiB request); API key and operator-only cancellation/replay.
- Unique `(client_id, idempotency_key)` in PostgreSQL; identical request returns same job, changed request returns 409.
- PostgreSQL `FOR UPDATE SKIP LOCKED` multi-process queue; DB server time for due times, leases and heartbeats.
- Lease generation fencing and deadline: stale worker cannot finalize. Heartbeat cannot extend an attempt past `timeout_seconds`. Each trusted handler runs in a child process terminated at timeout.
- Retryable vs permanent failures, bounded exponential backoff/jitter, permanent DLQ (`dead` jobs), append-only attempt and event history, explicit replay creating a new job with `replay_of` reference.
- Database down **fails closed**; workers wait and do not use memory fallback. External side effects **are not exactly-once**.

## Local setup (Docker Compose or compatible Podman Compose)

1. Create `.env` from `.env.example`. Generate three distinct secrets locally using `python -c "import secrets; print(secrets.token_urlsafe(40))"` and set `POSTGRES_PASSWORD`, `ASYNCFORGE_API_KEY`, `ASYNCFORGE_OPERATOR_KEY`. Never commit `.env`.
2. `docker compose up -d --build --scale worker=4` (Podman equivalent available where installed). Migration service must exit 0 before API/worker start.
3. `docker compose ps` and `curl http://127.0.0.1:8000/health/ready` (should be `{"status":"ready"}`).
4. Example request (replace the environment variable `ASYNCFORGE_API_KEY` with your *local* secret, never paste into logs):

```bash
curl -i -X POST http://127.0.0.1:8000/v1/jobs \
  -H "X-API-Key: $ASYNCFORGE_API_KEY" \
  -H "Idempotency-Key: hash-demo-001" \
  -H 'Content-Type: application/json' \
  --data '{"task_type":"demo.sha256","payload":{"text":"hello"}}'
```

`POST /v1/jobs` (201 created / 200 identical repeat / 409 changed request), `GET /v1/jobs`, `GET /v1/jobs/{id}`, `GET /v1/jobs/{id}/attempts`, `POST /v1/jobs/{id}/cancel`, `POST /v1/jobs/{id}/replay`. Operator writes also require `X-Operator-Key`; replay is restricted to `dead` and creates a **new** ID with independent attempt budget and event lineage. Existing idempotency key stays mapped to the original job. `run_at` requires ISO 8601 with timezone; results and retry state are persistent.

`/docs` serves the OpenAPI description; do not expose this service to the internet without TLS, network ACL, rate limits, stronger identity controls and audited secret management.

## Development/testing

```bash
python -m pip install -e '.[dev]'
ruff check .
pytest -q tests/test_unit.py
# For *isolated disposable* PostgreSQL database named asyncforge_test:
export DATABASE_URL='postgresql+psycopg://asyncforge:<test-password>@127.0.0.1:5433/asyncforge_test'
export ASYNCFORGE_API_KEY='<random local secret length >=24>'
export ASYNCFORGE_OPERATOR_KEY='<another random secret length >=24>'
alembic upgrade head
pytest -q tests/test_integration.py
```

To start isolated PostgreSQL services for local integration and benchmark only:

```bash
docker compose -f compose.dev.yaml up -d
# Test DB: postgresql+psycopg://asyncforge:local_test_only@127.0.0.1:5433/asyncforge_test
# Benchmark DB: postgresql+psycopg://asyncforge:local_bench_only@127.0.0.1:5434/asyncforge_bench
```

Integration tests **TRUNCATE tables**; the destructive fixture requires the **exact** `asyncforge_test` database name and a loopback/test-service hostname, not a fuzzy URL substring. If PostgreSQL is absent or migrations were not applied, integration tests **FAIL rather than SKIP**. Never point them at a production database or Neon. CI config uses an isolated PostgreSQL service container if a runner really executes it. A CI YAML file is not passing evidence; private-repository Actions can consume billable minutes. The workflow is intentionally **manual-only** pending owner verification of budget and quota. Do not dispatch until the account's zero-spend budget is verified.

Separate benchmark (destructive on *its own* isolated `asyncforge_bench` DB only):

```bash
DATABASE_URL='postgresql+psycopg://asyncforge:<password>@127.0.0.1:5434/asyncforge_bench' alembic upgrade head
DATABASE_URL='postgresql+psycopg://asyncforge:<password>@127.0.0.1:5434/asyncforge_bench' python benchmarks/measure.py --count 1000 --workers 1 2 4
```

Benchmark reports real elapsed wall time (including worker start), throughput, end-to-end latency p50/p95 from job creation to last update, per-run terminal states and retries. The engine deliberately runs every trusted task in a bounded child process, and this overhead is included in the measurement. Runs require exactly 1,000 successful jobs and zero retries/dead jobs (for the deterministic sha256 workload); an incomplete run **raises an error** instead of reporting false success. Never conflate this with correctness acceptance.

## Reliability and operational boundaries

- Lease expiration permits a second worker to execute the same task; `lease_generation` prevents a stale DB completion but not duplicated **external** side effects. Do not use unprotected publishing or transfers.
- SIGTERM stops acquiring new tasks, finishes a running child if it completes within the grace period; otherwise terminates it and lets lease recovery occur. SIGKILL cannot be graceful.
- Job deadline is a hard cap even if heartbeats continue; the child is terminated, retries then apply if allowed.
- Secrets excluded from logs. Only safe error codes exposed, not handler exception strings or payloads. Use a dedicated, least-privilege database and local-only bind.
- Migrations run from empty PostgreSQL. Downgrading schema deletes records and is **not** a recovery plan; back up before real migrations.
- Current limitations: no HA, cron, RBAC multi-tenancy, backpressure admission control, telemetry exporter or automatic external deduplication. No production deployment verified.

See [ARCHITECTURE.md](ARCHITECTURE.md), [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) and [test code](tests/). The original design docs contain some proposed options; **this README and current implementation are authoritative for delivered behavior**.

### Windows PowerShell example

```powershell
$env:ASYNCFORGE_API_KEY = 'YOUR_LOCAL_RANDOM_SECRET'
$body = @{ task_type = 'demo.sha256'; payload = @{ text = 'hello' } } | ConvertTo-Json -Depth 5
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/v1/jobs' `
  -Headers @{ 'X-API-Key' = $env:ASYNCFORGE_API_KEY; 'Idempotency-Key' = 'ps-demo-001' } `
  -ContentType 'application/json' -Body $body
```

Manual real outage test: start only `compose.dev.yaml`'s disposable PostgreSQL test service, run migrations against `asyncforge_test`, then set `ALLOW_DESTRUCTIVE_CHAOS=1` and run `pytest -q tests/test_outage_manual.py`. The test stops and starts **only** the local `postgres-test` service; never use a production database.

## GitHub Actions isolated acceptance (not yet executed)

The private-repository workflow `.github/workflows/ci.yml` has only **manual `workflow_dispatch`** to preserve the 0 TL policy while quota/billing is unverified. Do not enable auto-scheduled or continuous cloud runs without an explicit account-budget check. The workflow contains two jobs:

1. Ruff, Python compilation and five local unit tests.
2. A fresh `postgres:16` *service container*: migration upgrade + schema inspection, all eight non-skippable real PostgreSQL integration tests, a real local HTTP API/worker smoke test, then a controlled stop/restart of **only this exact test service container**. The test verifies its Docker image and `POSTGRES_DB=asyncforge_test` marker before shutdown.

After those correctness gates, the optional boolean workflow input `benchmark=true` creates a separate `asyncforge_bench` database and measures 1,000 tasks independently with 1, 2 and 4 workers. It uploads the resulting JSON only if actual measurements succeed. This optional benchmark can use substantial runner minutes; leave it off until free quota is confirmed. Both test and benchmark use synthetic handlers with no external network/production effects.

**Current evidence:** The workflow is configured but has **not run on GitHub**, and no new repository has been created. The developer's local environment has no Docker, Podman or PostgreSQL daemon, so only the unit/static checks in `TEST_REPORT.md` were executed. The `/health/ready` response is still 503 without PostgreSQL; do not call this deployment-ready. The previous local git commit `fcbfedd` is superseded by subsequent fixes once committed, not by a remote GitHub commit.
