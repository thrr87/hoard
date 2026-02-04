from __future__ import annotations

import os
import socket
import threading
import time
import uuid
from datetime import datetime, timedelta
from typing import Optional

from hoard.core.db.connection import connect
from hoard.core.db.writer import WriteCoordinator
from hoard.core.memory.embeddings import encode_embedding


class Worker:
    def __init__(self, *, db_path, config: dict, writer: WriteCoordinator) -> None:
        self._db_path = db_path
        self._config = config
        self._writer = writer
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._worker_id = None

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)

    def _run(self) -> None:
        self._worker_id = f"worker-{socket.gethostname()}-{threading.get_ident()}"
        poll_interval = int(self._config.get("write", {}).get("worker", {}).get("poll_interval_ms", 1000)) / 1000
        job_timeout = int(self._config.get("write", {}).get("worker", {}).get("job_timeout_seconds", 60))
        lease_seconds = int(self._config.get("write", {}).get("worker", {}).get("lease_duration_seconds", 60))

        while not self._stop.is_set():
            if not self._writer.submit(self._acquire_or_renew_lease, lease_seconds):
                time.sleep(poll_interval)
                continue

            self._writer.submit(self._requeue_stuck_jobs, job_timeout)
            job = self._writer.submit(self._claim_job)
            if not job:
                time.sleep(poll_interval)
                continue

            job_id = job["id"]
            job_type = job["job_type"]
            memory_id = job["memory_id"]

            error = None
            try:
                if job_type == "embed_memory":
                    self._process_embed(memory_id)
                elif job_type == "detect_duplicates":
                    self._process_duplicates(memory_id)
                elif job_type == "detect_conflicts":
                    self._process_conflicts(memory_id)
            except Exception as exc:
                error = str(exc)

            if error:
                self._writer.submit(self._fail_job, job_id, error)
            else:
                self._writer.submit(self._complete_job, job_id)

    def _acquire_or_renew_lease(self, conn, lease_seconds: int) -> bool:
        if not self._worker_id:
            self._worker_id = f"worker-{socket.gethostname()}-{os.getpid()}-{id(self)}"
        now = datetime.utcnow().isoformat(timespec="seconds")
        expires_at = (datetime.utcnow() + timedelta(seconds=lease_seconds)).isoformat(timespec="seconds")
        cursor = conn.execute(
            """
            INSERT INTO worker_lease (id, worker_id, hostname, pid, acquired_at, heartbeat_at, expires_at)
            VALUES (1, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                worker_id = excluded.worker_id,
                hostname = excluded.hostname,
                pid = excluded.pid,
                acquired_at = excluded.acquired_at,
                heartbeat_at = excluded.heartbeat_at,
                expires_at = excluded.expires_at
            WHERE worker_lease.expires_at < ?
            """,
            (
                self._worker_id,
                socket.gethostname(),
                os.getpid(),
                now,
                now,
                expires_at,
                now,
            ),
        )
        if cursor.rowcount == 0:
            # Attempt heartbeat if we already own lease
            row = conn.execute("SELECT worker_id FROM worker_lease WHERE id = 1").fetchone()
            if not row or row[0] != self._worker_id:
                return False
            conn.execute(
                "UPDATE worker_lease SET heartbeat_at = ?, expires_at = ? WHERE id = 1",
                (now, expires_at),
            )
        return True

    def _requeue_stuck_jobs(self, conn, timeout_seconds: int) -> None:
        threshold = (datetime.utcnow() - timedelta(seconds=timeout_seconds)).isoformat(timespec="seconds")
        rows = conn.execute(
            """
            SELECT id, retry_count, max_retries
            FROM background_jobs
            WHERE status = 'running' AND started_at < ?
            """,
            (threshold,),
        ).fetchall()
        for row in rows:
            if row["retry_count"] >= row["max_retries"]:
                conn.execute(
                    """
                    UPDATE background_jobs
                    SET status = 'failed', completed_at = ?, error = 'Job timed out'
                    WHERE id = ?
                    """,
                    (datetime.utcnow().isoformat(timespec="seconds"), row["id"]),
                )
            else:
                conn.execute(
                    """
                    UPDATE background_jobs
                    SET status = 'pending', retry_count = retry_count + 1, started_at = NULL
                    WHERE id = ?
                    """,
                    (row["id"],),
                )

    def _claim_job(self, conn) -> Optional[dict]:
        row = conn.execute(
            """
            SELECT id FROM background_jobs
            WHERE status = 'pending'
            ORDER BY priority DESC, created_at
            LIMIT 1
            """,
        ).fetchone()
        if not row:
            return None
        job_id = row["id"]
        conn.execute(
            """
            UPDATE background_jobs
            SET status = 'running', started_at = ?
            WHERE id = ?
            """,
            (datetime.utcnow().isoformat(timespec="seconds"), job_id),
        )
        job = conn.execute("SELECT * FROM background_jobs WHERE id = ?", (job_id,)).fetchone()
        return dict(job) if job else None

    def _complete_job(self, conn, job_id: str) -> None:
        conn.execute(
            """
            UPDATE background_jobs
            SET status = 'completed', completed_at = ?
            WHERE id = ?
            """,
            (datetime.utcnow().isoformat(timespec="seconds"), job_id),
        )

    def _fail_job(self, conn, job_id: str, error: str) -> None:
        conn.execute(
            """
            UPDATE background_jobs
            SET status = 'failed', completed_at = ?, error = ?
            WHERE id = ?
            """,
            (datetime.utcnow().isoformat(timespec="seconds"), error, job_id),
        )

    def _process_embed(self, memory_id: str) -> None:
        conn = connect(self._db_path)
        try:
            row = conn.execute("SELECT content FROM memories WHERE id = ?", (memory_id,)).fetchone()
            if not row:
                return
            content = row["content"]
        finally:
            conn.close()

        model_cfg = self._config.get("write", {}).get("embeddings", {}).get("active_model", {})
        model_name = model_cfg.get("name", "sentence-transformers/all-MiniLM-L6-v2")
        model_version = model_cfg.get("version", "2.0.0")
        dimensions = int(model_cfg.get("dimensions", 384))

        from sentence_transformers import SentenceTransformer

        model = SentenceTransformer(model_name)
        vector = model.encode([content], normalize_embeddings=True)[0]
        blob = encode_embedding(vector, dimensions)
        embedded_at = datetime.utcnow().isoformat(timespec="seconds")

        def _store(conn):
            conn.execute(
                """
                INSERT INTO memory_embeddings
                (memory_id, embedding, model_name, model_version, dimensions, embedded_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(memory_id) DO UPDATE SET
                    embedding = excluded.embedding,
                    model_name = excluded.model_name,
                    model_version = excluded.model_version,
                    dimensions = excluded.dimensions,
                    embedded_at = excluded.embedded_at
                """,
                (memory_id, blob, model_name, model_version, dimensions, embedded_at),
            )
            conn.execute(
                """
                INSERT INTO memory_events (id, memory_id, event_type, event_at, actor, event_data)
                VALUES (?, ?, 'embedding_added', ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    memory_id,
                    embedded_at,
                    "worker",
                    None,
                ),
            )

        self._writer.submit(_store)

    def _process_duplicates(self, memory_id: str) -> None:
        # Placeholder: detection not implemented yet
        return

    def _process_conflicts(self, memory_id: str) -> None:
        # Placeholder: detection not implemented yet
        return
