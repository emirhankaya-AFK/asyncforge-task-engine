"""Runs separately from correctness tests: 1000 identical synthetic tasks per configuration."""
import argparse
import json
import os
import platform
import subprocess
import sys
import time

from sqlalchemy import func, select, text
from sqlalchemy.engine import make_url

from asyncforge.db import Attempt, Job, session_factory


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--count", type=int, default=1000)
    p.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4])
    p.add_argument("--output", default="benchmarks/results/local.json")
    p.add_argument("--max-seconds", type=int, default=900)
    args = p.parse_args()
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        raise SystemExit("DATABASE_URL required")
    parsed = make_url(url)
    if parsed.database != "asyncforge_bench" or parsed.host not in ("localhost", "127.0.0.1", "postgres-bench"):
        raise SystemExit("Refusing to benchmark outside isolated local asyncforge_bench DB")
    report = {"environment": {"python": sys.version, "platform": platform.platform(),
                              "machine": platform.machine(), "cpu_count": os.cpu_count(),
                              "database": "isolated PostgreSQL; DATABASE_URL redacted"},
              "task": "demo.sha256 constant 256-byte ASCII string", "count": args.count, "runs": []}
    if args.count < 1 or args.count > 10000 or any(n not in (1, 2, 4) for n in args.workers):
        raise SystemExit("Invalid count or worker configuration")
    for n in args.workers:
        with session_factory().begin() as db:
            db.execute(text("TRUNCATE job_events, job_attempts, jobs RESTART IDENTITY CASCADE"))
            jobs = [Job(client_id="bench", idempotency_key=f"bench-{i}", request_hash="bench",
                        task_type="demo.sha256", payload={"text": "x" * 256}, queue="default") for i in range(args.count)]
            db.add_all(jobs)
        began = time.monotonic()
        ps = [subprocess.Popen([sys.executable, "-m", "asyncforge.worker", "--worker-id", f"benchmark-{i}", "--max-idle", "4"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) for i in range(n)]
        try:
            deadline = began + args.max_seconds
            while time.monotonic() < deadline:
                with session_factory().begin() as db:
                    results = dict(db.execute(select(Job.state, func.count()).group_by(Job.state)).all())
                if results.get("succeeded", 0) + results.get("dead", 0) == args.count:
                    break
                time.sleep(.15)
            duration = time.monotonic() - began
            with session_factory().begin() as db:
                results = dict(db.execute(select(Job.state, func.count()).group_by(Job.state)).all())
                retries = db.scalar(select(func.count()).select_from(Attempt).where(Attempt.outcome == "retry"))
                latencies = [float(v) for v in db.scalars(select(func.extract("epoch", Job.updated_at - Job.created_at) * 1000))]
            latencies.sort()
            if not (results.get("succeeded", 0) == args.count and results.get("dead", 0) == 0 and retries == 0):
                raise RuntimeError(f"Benchmark invalid for {n} workers: states={results}, retries={retries}")
            report["runs"].append({"workers": n, "elapsed_seconds": round(duration, 3),
                                    "throughput_jobs_per_second": round(results.get("succeeded", 0) / duration, 2),
                                    "p50_latency_ms": round(latencies[len(latencies)//2], 2),
                                    "p95_latency_ms": round(latencies[min(len(latencies)-1, int(len(latencies)*.95))], 2),
                                    "states": results, "retries": retries,
                                    "all_terminal": sum(results.get(x, 0) for x in ("succeeded", "dead")) == args.count})
        finally:
            for proc in ps:
                if proc.poll() is None:
                    proc.terminate()
                try:
                    proc.wait(timeout=12)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()
