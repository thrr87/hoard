from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from hoard.core.db.connection import connect, initialize_db
from hoard.core.db.writer import WriteCoordinator
from hoard.core.worker import Worker


def _config() -> dict:
    return {
        "write": {
            "worker": {
                "poll_interval_ms": 10,
                "job_timeout_seconds": 1,
                "lease_duration_seconds": 60,
            }
        }
    }


def test_worker_lease(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    writer = WriteCoordinator(db_path=db_path)
    worker = Worker(db_path=db_path, config=_config(), writer=writer)

    assert worker._acquire_or_renew_lease(conn, 60)

    worker2 = Worker(db_path=db_path, config=_config(), writer=writer)
    assert not worker2._acquire_or_renew_lease(conn, 60)

    conn.close()
    writer.stop()


def test_stuck_recovery(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    writer = WriteCoordinator(db_path=db_path)
    worker = Worker(db_path=db_path, config=_config(), writer=writer)

    started_at = (datetime.utcnow() - timedelta(seconds=120)).isoformat(timespec="seconds")
    conn.execute(
        """
        INSERT INTO background_jobs (id, job_type, memory_id, status, priority, created_at, started_at, retry_count, max_retries)
        VALUES ('job-1', 'embed_memory', NULL, 'running', 0, ?, ?, 0, 1)
        """,
        (started_at, started_at),
    )
    conn.commit()

    worker._requeue_stuck_jobs(conn, timeout_seconds=1)
    row = conn.execute("SELECT status FROM background_jobs WHERE id = 'job-1'").fetchone()
    assert row[0] in {"pending", "failed"}

    conn.close()
    writer.stop()
