"""Validate the actual empty-database upgrade, not merely generated SQL."""
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
import os


def main():
    url = os.environ["DATABASE_URL"]
    parsed = make_url(url)
    if parsed.database != "asyncforge_test" or parsed.host not in ("localhost", "127.0.0.1"):
        raise SystemExit("Migration validation must target isolated local asyncforge_test")
    engine = create_engine(url, pool_pre_ping=True)
    with engine.connect() as conn:
        assert conn.scalar(text("SELECT current_database()")) == "asyncforge_test"
        assert conn.scalar(text("SELECT version_num FROM alembic_version")) == "0001"
        inspector = inspect(conn)
        for table in ("jobs", "job_attempts", "job_events"):
            assert inspector.has_table(table), table
        constraints = {c["name"] for c in inspector.get_unique_constraints("jobs")}
        assert "uq_job_idempotency" in constraints, constraints
        assert {"ix_jobs_ready", "ix_jobs_expired"}.issubset(
            {idx["name"] for idx in inspector.get_indexes("jobs")}
        )
    print("PASS: real PostgreSQL migration version 0001, tables, unique constraint, queue indexes")


if __name__ == "__main__":
    main()
