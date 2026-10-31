# AsyncForge verification report — 2026-10-31

## Sources and access

- Read the previous `ARCHITECTURE.md`, `IMPLEMENTATION_PLAN.md`, source code, migration, 27 tracked baseline files and the previous test report before editing.
- Previous commit `fcbfedd28951b7d7ad85e97d57a529b3027848aa` was verified locally via `git log`; previous Git bundle verified with `git bundle verify`.
- AFK-connected GitHub account lists 71 accessible repositories and does not list `asyncforge-task-engine`. Direct authenticated repository lookup returned HTTP 404. **This means not visible to this credential, not proof of global name availability.**
- GitHub connector lacks a `create_repository` action. Connected Browser can read existing tabs only, not navigate or create repositories. No repo or GitHub commit was created; CI has no run URL.

## Local environment (observed)

- Debian GNU/Linux 13 (trixie), x86_64, Python 3.13 (the Dockerfile specifies Python 3.12).
- `docker`, `podman`, `postgres`, `pg_ctl`, `initdb`, `psql`, `ruff`, `psycopg` were not found; `pg_config` and Python tooling are present.
- No cached PostgreSQL `.deb` packages were available. Only `libreoffice-sdbc-postgresql`, a client driver, was installed by dpkg; this is **not a PostgreSQL server**.
- DNS lookup of `deb.debian.org` and `pypi.org` failed with `gaierror [Errno -3] Temporary failure in name resolution`. Did **not** repeat package-install attempts; the network obstacle remains.

## Commands actually executed and results

| Check | Actual result | Scope |
|---|---|---|
| `PYTHONPATH=src pytest -q tests/test_unit.py` | **5 passed** | Real local unit tests, not PostgreSQL |
| `PYTHONPATH=src python -m compileall -q src tests migrations benchmarks` | **PASS** | Python syntax compilation |
| `DATABASE_URL=postgresql+psycopg://.../asyncforge_test PYTHONPATH=src alembic upgrade head --sql` | **PASS** | Offline PostgreSQL SQL generation ONLY; not applied to server |
| Parse `compose.yaml`, `compose.dev.yaml`, `.github/workflows/ci.yml` with PyYAML | **PASS** | YAML syntax only; Docker runtime not validated |
| `PYTHONPATH=src pytest -q -x tests/test_integration.py` without DB | **Explicit FAIL (exit 1)** | Missing PostgreSQL is no longer misreported as skipped/pass |
| `PYTHONPATH=src pytest --collect-only -q tests/` | **14 collected** | 5 unit, 8 PostgreSQL integration, 1 isolated outage test |
| Real PostgreSQL migration and API readiness 200 | **NOT RUN** | No server/driver |
| Independent 4-worker, SIGKILL, lease/fencing, retry, DLQ, scheduling, SIGTERM, DB outage | **NOT RUN** | Real PostgreSQL required |
| Actual Docker Compose startup and shutdown | **NOT RUN** | No Docker/Podman binary |
| `ruff check .` | **NOT RUN** | Ruff unavailable |
| GitHub Actions CI | **NOT RUN** | No target repository exists in connected account |
| 1000-job benchmark with 1/2/4 workers | **NOT RUN** | No PostgreSQL; **no fabricated numbers** |

## Delivered improvements since fcbfedd

- Real PostgreSQL integration tests **fail closed** without the exact isolated `asyncforge_test` database and a local/test-service host. A CI test job cannot be marked green through all test skips.
- Migration verifier queries the actual Alembic version, tables, unique idempotency constraint and queue indexes.
- Real HTTP test launches independent Uvicorn and worker processes, checks liveness/readiness/401/201/200/409 and polls a persisted terminal SHA-256 result.
- Expanded integration checks: four **distinct** worker IDs must claim tasks; positive and negative heartbeat fencing, child process hard deadline, retries, crash recovery and SIGTERM.
- Isolated service-container outage test requires an exact Docker container ID and verifies the image is PostgreSQL 16 and its database is `asyncforge_test`; explicitly checks readiness failure and recovery. It cannot stop unrelated containers.
- Manual-only GitHub Actions workflow for 0 TL control, with the two actual acceptance jobs and an optional benchmark after acceptance. No billed CI run was initiated.
- Benchmark verifies that each 1/2/4-worker run has exactly the requested number of completed jobs, zero dead and zero retries. Invalid runs fail rather than emit fictitious throughput numbers; database target must exactly match local `asyncforge_bench`.

## Acceptance status

**NOT ACCEPTED YET.** Local source and five unit tests are available. Real PostgreSQL, multi-process ownership, migration, actual CI and benchmark remain unverified. No production Neon databases, social integrations or user credentials were used. No paid account changes were made.

### Required smallest external dependency

Create an **empty PRIVATE** repository named `asyncforge-task-engine` under the logged-in **emirhankaya-AFK** account. Do not initialize a README, license or `.gitignore` on GitHub. The GitHub repository creation capability is not available in Chat-mode connector or read-only Browser. After that, pushing can use the local Git bundle from a trusted machine with an authenticated GitHub CLI, HTTPS credential helper or SSH key. Do not insert a PAT into the remote URL or conversation. Before dispatching the workflow, confirm private Actions free quota and spending limit = zero.
