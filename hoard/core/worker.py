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
from hoard.core.memory.model_cache import get_sentence_transformer
from hoard.core.memory.predicates import active_memory_conditions


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

        model = get_sentence_transformer(model_name)
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
        embeddings_enabled = bool(
            self._config.get("write", {}).get("embeddings", {}).get("enabled", False)
        )
        if not embeddings_enabled:
            return

        threshold = float(
            self._config.get("write", {}).get("duplicates", {}).get("similarity_threshold", 0.85)
        )
        model_cfg = self._config.get("write", {}).get("embeddings", {}).get("active_model", {})
        model_name = model_cfg.get("name", "sentence-transformers/all-MiniLM-L6-v2")
        dimensions = int(model_cfg.get("dimensions", 384))

        conn = connect(self._db_path)
        try:
            emb_row = conn.execute(
                "SELECT embedding, model_name, dimensions FROM memory_embeddings WHERE memory_id = ?",
                (memory_id,),
            ).fetchone()
            if not emb_row:
                return
            if emb_row["model_name"] != model_name or emb_row["dimensions"] != dimensions:
                return

            from array import array as float_array

            query_arr = float_array("f")
            query_arr.frombytes(emb_row["embedding"])
            if len(query_arr) != dimensions:
                return
            query_vec = list(query_arr)

            now = datetime.utcnow().isoformat(timespec="seconds")
            src_conditions, src_params = active_memory_conditions(now, table_alias="m")
            src_conditions.append("m.id = ?")
            src_params.append(memory_id)
            src_where = " AND ".join(src_conditions)
            mem_row = conn.execute(
                f"SELECT m.scope_type, m.scope_id FROM memories m WHERE {src_where}",
                src_params,
            ).fetchone()
            if not mem_row:
                return
            conditions, params = active_memory_conditions(now)
            conditions.append("m.id != ?")
            params.append(memory_id)
            if mem_row["scope_id"] is None:
                conditions.append("m.scope_type = ?")
                params.append(mem_row["scope_type"])
                conditions.append("m.scope_id IS NULL")
            else:
                conditions.append("m.scope_type = ?")
                params.append(mem_row["scope_type"])
                conditions.append("m.scope_id = ?")
                params.append(mem_row["scope_id"])

            where_clause = " AND ".join(conditions)
            candidates = conn.execute(
                f"""
                SELECT e.memory_id, e.embedding, e.model_name, e.dimensions
                FROM memory_embeddings e
                JOIN memories m ON m.id = e.memory_id
                WHERE {where_clause}
                """,
                params,
            ).fetchall()

            duplicates = []
            for cand in candidates:
                if cand["model_name"] != model_name or cand["dimensions"] != dimensions:
                    continue
                cand_arr = float_array("f")
                cand_arr.frombytes(cand["embedding"])
                if len(cand_arr) != dimensions:
                    continue
                similarity = sum(a * b for a, b in zip(query_vec, cand_arr))
                if similarity >= threshold:
                    duplicates.append((cand["memory_id"], similarity))
        finally:
            conn.close()

        if not duplicates:
            return

        def _store_duplicates(conn):
            for other_id, similarity in duplicates:
                already = conn.execute(
                    """
                    SELECT d.id FROM memory_duplicates d
                    JOIN duplicate_members dm1 ON dm1.duplicate_id = d.id
                    JOIN duplicate_members dm2 ON dm2.duplicate_id = d.id
                    WHERE dm1.memory_id = ? AND dm2.memory_id = ?
                      AND d.resolved_at IS NULL
                    """,
                    (memory_id, other_id),
                ).fetchone()
                if already:
                    continue
                dup_id = str(uuid.uuid4())
                now_ts = datetime.utcnow().isoformat(timespec="seconds")
                conn.execute(
                    "INSERT INTO memory_duplicates (id, detected_at, similarity) VALUES (?, ?, ?)",
                    (dup_id, now_ts, similarity),
                )
                conn.execute(
                    "INSERT INTO duplicate_members (duplicate_id, memory_id, is_canonical) VALUES (?, ?, 1)",
                    (dup_id, other_id),
                )
                conn.execute(
                    "INSERT INTO duplicate_members (duplicate_id, memory_id, is_canonical) VALUES (?, ?, 0)",
                    (dup_id, memory_id),
                )

        self._writer.submit(_store_duplicates)

    def _process_conflicts(self, memory_id: str) -> None:
        conn = connect(self._db_path)
        try:
            now = datetime.utcnow().isoformat(timespec="seconds")
            src_conditions, src_params = active_memory_conditions(now, table_alias="m")
            src_conditions.append("m.id = ?")
            src_params.append(memory_id)
            src_where = " AND ".join(src_conditions)
            mem_row = conn.execute(
                f"SELECT m.slot, m.scope_type, m.scope_id FROM memories m WHERE {src_where}",
                src_params,
            ).fetchone()
            if not mem_row or not mem_row["slot"]:
                return

            slot = mem_row["slot"]
            scope_type = mem_row["scope_type"]
            scope_id = mem_row["scope_id"]

            now = datetime.utcnow().isoformat(timespec="seconds")
            conditions, params = active_memory_conditions(now)
            conditions.append("m.id != ?")
            params.append(memory_id)
            conditions.append("m.slot = ?")
            params.append(slot)
            conditions.append("m.scope_type = ?")
            params.append(scope_type)
            if scope_id is None:
                conditions.append("m.scope_id IS NULL")
            else:
                conditions.append("m.scope_id = ?")
                params.append(scope_id)

            where_clause = " AND ".join(conditions)
            others = conn.execute(
                f"SELECT m.id FROM memories m WHERE {where_clause}",
                params,
            ).fetchall()

            if not others:
                return

            other_ids = [row["id"] for row in others]

            existing_conflict_id = None
            for other_id in other_ids:
                row = conn.execute(
                    """
                    SELECT cm.conflict_id FROM conflict_members cm
                    JOIN memory_conflicts c ON c.id = cm.conflict_id
                    WHERE cm.memory_id = ? AND c.resolved_at IS NULL
                      AND c.slot = ? AND c.scope_type = ?
                    """,
                    (other_id, slot, scope_type),
                ).fetchone()
                if row:
                    existing_conflict_id = row["conflict_id"]
                    break
        finally:
            conn.close()

        def _store_conflict(conn):
            now_ts = datetime.utcnow().isoformat(timespec="seconds")
            if existing_conflict_id:
                already = conn.execute(
                    "SELECT 1 FROM conflict_members WHERE conflict_id = ? AND memory_id = ?",
                    (existing_conflict_id, memory_id),
                ).fetchone()
                if not already:
                    conn.execute(
                        "INSERT INTO conflict_members (conflict_id, memory_id, nli_label, added_at) VALUES (?, ?, NULL, ?)",
                        (existing_conflict_id, memory_id, now_ts),
                    )
            else:
                conflict_id = str(uuid.uuid4())
                conn.execute(
                    "INSERT INTO memory_conflicts (id, slot, scope_type, scope_id, detected_at) VALUES (?, ?, ?, ?, ?)",
                    (conflict_id, slot, scope_type, scope_id, now_ts),
                )
                for mid in other_ids:
                    conn.execute(
                        "INSERT INTO conflict_members (conflict_id, memory_id, nli_label, added_at) VALUES (?, ?, NULL, ?)",
                        (conflict_id, mid, now_ts),
                    )
                conn.execute(
                    "INSERT INTO conflict_members (conflict_id, memory_id, nli_label, added_at) VALUES (?, ?, NULL, ?)",
                    (conflict_id, memory_id, now_ts),
                )

        self._writer.submit(_store_conflict)
