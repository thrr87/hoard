from __future__ import annotations

import uuid
from array import array
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


def _detection_config(embeddings_enabled: bool = True) -> dict:
    return {
        "write": {
            "worker": {
                "poll_interval_ms": 10,
                "job_timeout_seconds": 1,
                "lease_duration_seconds": 60,
            },
            "embeddings": {
                "enabled": embeddings_enabled,
                "active_model": {
                    "name": "test-model",
                    "version": "1.0",
                    "dimensions": 4,
                },
            },
            "duplicates": {
                "similarity_threshold": 0.85,
            },
        }
    }


def _make_embedding(values: list[float]) -> bytes:
    """Encode a float vector as little-endian float32 bytes."""
    arr = array("f", values)
    return arr.tobytes()


def _insert_memory(conn, memory_id: str, content: str, slot: str | None = None,
                    scope_type: str = "user", scope_id: str | None = None) -> None:
    now = datetime.utcnow().isoformat(timespec="seconds")
    conn.execute(
        """
        INSERT INTO memories (id, content, memory_type, slot, scope_type, scope_id,
                              source_agent, created_at, sensitivity)
        VALUES (?, ?, 'fact', ?, ?, ?, 'test', ?, 'normal')
        """,
        (memory_id, content, slot, scope_type, scope_id, now),
    )
    conn.execute("INSERT INTO memory_counters (memory_id) VALUES (?)", (memory_id,))
    conn.commit()


def _insert_embedding(conn, memory_id: str, embedding: bytes, model_name: str = "test-model",
                       dimensions: int = 4) -> None:
    now = datetime.utcnow().isoformat(timespec="seconds")
    conn.execute(
        """
        INSERT INTO memory_embeddings (memory_id, embedding, model_name, model_version, dimensions, embedded_at)
        VALUES (?, ?, ?, '1.0', ?, ?)
        """,
        (memory_id, embedding, model_name, dimensions, now),
    )
    conn.commit()


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


def test_duplicate_detection(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    writer = WriteCoordinator(db_path=db_path)
    worker = Worker(db_path=db_path, config=_detection_config(), writer=writer)

    mid1 = str(uuid.uuid4())
    mid2 = str(uuid.uuid4())
    vec = [0.5, 0.5, 0.5, 0.5]  # identical vectors → similarity 1.0
    blob = _make_embedding(vec)

    _insert_memory(conn, mid1, "duplicate content A")
    _insert_memory(conn, mid2, "duplicate content A")
    _insert_embedding(conn, mid1, blob)
    _insert_embedding(conn, mid2, blob)

    worker._process_duplicates(mid2)

    dup_rows = conn.execute("SELECT * FROM memory_duplicates").fetchall()
    assert len(dup_rows) == 1
    assert dup_rows[0]["similarity"] >= 0.99

    members = conn.execute(
        "SELECT memory_id, is_canonical FROM duplicate_members ORDER BY is_canonical DESC"
    ).fetchall()
    assert len(members) == 2
    member_ids = {m["memory_id"] for m in members}
    assert mid1 in member_ids
    assert mid2 in member_ids

    conn.close()
    writer.stop()


def test_duplicate_detection_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    writer = WriteCoordinator(db_path=db_path)
    worker = Worker(db_path=db_path, config=_detection_config(), writer=writer)

    mid1 = str(uuid.uuid4())
    mid2 = str(uuid.uuid4())
    blob = _make_embedding([0.5, 0.5, 0.5, 0.5])

    _insert_memory(conn, mid1, "same content")
    _insert_memory(conn, mid2, "same content")
    _insert_embedding(conn, mid1, blob)
    _insert_embedding(conn, mid2, blob)

    worker._process_duplicates(mid2)
    worker._process_duplicates(mid2)

    dup_rows = conn.execute("SELECT * FROM memory_duplicates").fetchall()
    assert len(dup_rows) == 1

    conn.close()
    writer.stop()


def test_duplicate_detection_skips_without_embedding(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    writer = WriteCoordinator(db_path=db_path)
    worker = Worker(db_path=db_path, config=_detection_config(), writer=writer)

    mid = str(uuid.uuid4())
    _insert_memory(conn, mid, "no embedding content")

    # Should complete without error (no-op)
    worker._process_duplicates(mid)

    dup_rows = conn.execute("SELECT * FROM memory_duplicates").fetchall()
    assert len(dup_rows) == 0

    conn.close()
    writer.stop()


def test_duplicate_detection_skips_mismatched_model(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    writer = WriteCoordinator(db_path=db_path)
    worker = Worker(db_path=db_path, config=_detection_config(), writer=writer)

    mid1 = str(uuid.uuid4())
    mid2 = str(uuid.uuid4())
    blob = _make_embedding([0.5, 0.5, 0.5, 0.5])

    _insert_memory(conn, mid1, "content A")
    _insert_memory(conn, mid2, "content B")
    _insert_embedding(conn, mid1, blob, model_name="other-model")
    _insert_embedding(conn, mid2, blob)

    worker._process_duplicates(mid2)

    dup_rows = conn.execute("SELECT * FROM memory_duplicates").fetchall()
    assert len(dup_rows) == 0

    conn.close()
    writer.stop()


def test_duplicate_detection_null_scope_id(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    writer = WriteCoordinator(db_path=db_path)
    worker = Worker(db_path=db_path, config=_detection_config(), writer=writer)

    mid1 = str(uuid.uuid4())
    mid2 = str(uuid.uuid4())
    mid3 = str(uuid.uuid4())
    blob = _make_embedding([0.5, 0.5, 0.5, 0.5])

    # mid1 and mid2: user scope (NULL scope_id)
    _insert_memory(conn, mid1, "user content", scope_type="user", scope_id=None)
    _insert_memory(conn, mid2, "user content", scope_type="user", scope_id=None)
    # mid3: project scope (different scope) — should NOT match
    _insert_memory(conn, mid3, "project content", scope_type="project", scope_id="proj1")
    _insert_embedding(conn, mid1, blob)
    _insert_embedding(conn, mid2, blob)
    _insert_embedding(conn, mid3, blob)

    worker._process_duplicates(mid2)

    members = conn.execute("SELECT memory_id FROM duplicate_members").fetchall()
    member_ids = {m["memory_id"] for m in members}
    assert mid1 in member_ids
    assert mid2 in member_ids
    assert mid3 not in member_ids

    conn.close()
    writer.stop()


def test_conflict_detection(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    writer = WriteCoordinator(db_path=db_path)
    worker = Worker(db_path=db_path, config=_detection_config(), writer=writer)

    mid1 = str(uuid.uuid4())
    mid2 = str(uuid.uuid4())

    _insert_memory(conn, mid1, "preferred dark mode", slot="pref:theme", scope_type="user")
    _insert_memory(conn, mid2, "preferred light mode", slot="pref:theme", scope_type="user")

    worker._process_conflicts(mid2)

    conflict_rows = conn.execute("SELECT * FROM memory_conflicts").fetchall()
    assert len(conflict_rows) == 1
    assert conflict_rows[0]["slot"] == "pref:theme"
    assert conflict_rows[0]["scope_type"] == "user"
    assert conflict_rows[0]["scope_id"] is None

    members = conn.execute("SELECT memory_id FROM conflict_members").fetchall()
    member_ids = {m["memory_id"] for m in members}
    assert mid1 in member_ids
    assert mid2 in member_ids

    conn.close()
    writer.stop()


def test_conflict_detection_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    writer = WriteCoordinator(db_path=db_path)
    worker = Worker(db_path=db_path, config=_detection_config(), writer=writer)

    mid1 = str(uuid.uuid4())
    mid2 = str(uuid.uuid4())

    _insert_memory(conn, mid1, "value A", slot="fact:test.key", scope_type="user")
    _insert_memory(conn, mid2, "value B", slot="fact:test.key", scope_type="user")

    worker._process_conflicts(mid2)
    worker._process_conflicts(mid2)

    conflict_rows = conn.execute("SELECT * FROM memory_conflicts").fetchall()
    assert len(conflict_rows) == 1

    members = conn.execute("SELECT memory_id FROM conflict_members").fetchall()
    assert len(members) == 2

    conn.close()
    writer.stop()


def test_conflict_appends_to_existing(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    writer = WriteCoordinator(db_path=db_path)
    worker = Worker(db_path=db_path, config=_detection_config(), writer=writer)

    mid1 = str(uuid.uuid4())
    mid2 = str(uuid.uuid4())
    mid3 = str(uuid.uuid4())

    _insert_memory(conn, mid1, "value A", slot="fact:test.key", scope_type="user")
    _insert_memory(conn, mid2, "value B", slot="fact:test.key", scope_type="user")

    worker._process_conflicts(mid2)

    conflict_rows = conn.execute("SELECT * FROM memory_conflicts").fetchall()
    assert len(conflict_rows) == 1

    # Third memory with same slot: should append to existing conflict, not create new
    _insert_memory(conn, mid3, "value C", slot="fact:test.key", scope_type="user")
    worker._process_conflicts(mid3)

    conflict_rows = conn.execute("SELECT * FROM memory_conflicts").fetchall()
    assert len(conflict_rows) == 1

    members = conn.execute("SELECT memory_id FROM conflict_members").fetchall()
    member_ids = {m["memory_id"] for m in members}
    assert mid1 in member_ids
    assert mid2 in member_ids
    assert mid3 in member_ids
    assert len(members) == 3

    conn.close()
    writer.stop()


def test_conflict_detection_skips_without_slot(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    writer = WriteCoordinator(db_path=db_path)
    worker = Worker(db_path=db_path, config=_detection_config(), writer=writer)

    mid = str(uuid.uuid4())
    _insert_memory(conn, mid, "no slot content", slot=None, scope_type="user")

    worker._process_conflicts(mid)

    conflict_rows = conn.execute("SELECT * FROM memory_conflicts").fetchall()
    assert len(conflict_rows) == 0

    conn.close()
    writer.stop()


def test_conflict_detection_null_scope_id(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    writer = WriteCoordinator(db_path=db_path)
    worker = Worker(db_path=db_path, config=_detection_config(), writer=writer)

    mid1 = str(uuid.uuid4())
    mid2 = str(uuid.uuid4())
    mid3 = str(uuid.uuid4())

    # mid1 and mid2: user scope (NULL scope_id), same slot
    _insert_memory(conn, mid1, "user A", slot="fact:test.key", scope_type="user", scope_id=None)
    _insert_memory(conn, mid2, "user B", slot="fact:test.key", scope_type="user", scope_id=None)
    # mid3: project scope with same slot — should NOT conflict with mid1/mid2
    _insert_memory(conn, mid3, "project A", slot="fact:test.key", scope_type="project", scope_id="proj1")

    worker._process_conflicts(mid2)

    members = conn.execute("SELECT memory_id FROM conflict_members").fetchall()
    member_ids = {m["memory_id"] for m in members}
    assert mid1 in member_ids
    assert mid2 in member_ids
    assert mid3 not in member_ids

    conn.close()
    writer.stop()


def test_duplicate_detection_skips_retracted_source(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    writer = WriteCoordinator(db_path=db_path)
    worker = Worker(db_path=db_path, config=_detection_config(), writer=writer)

    mid1 = str(uuid.uuid4())
    mid2 = str(uuid.uuid4())
    blob = _make_embedding([0.5, 0.5, 0.5, 0.5])

    _insert_memory(conn, mid1, "content A")
    _insert_memory(conn, mid2, "content A")
    _insert_embedding(conn, mid1, blob)
    _insert_embedding(conn, mid2, blob)

    # Retract the source memory
    now = datetime.utcnow().isoformat(timespec="seconds")
    conn.execute("UPDATE memories SET retracted_at = ? WHERE id = ?", (now, mid2))
    conn.commit()

    worker._process_duplicates(mid2)

    dup_rows = conn.execute("SELECT * FROM memory_duplicates").fetchall()
    assert len(dup_rows) == 0

    conn.close()
    writer.stop()


def test_conflict_detection_skips_retracted_source(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    writer = WriteCoordinator(db_path=db_path)
    worker = Worker(db_path=db_path, config=_detection_config(), writer=writer)

    mid1 = str(uuid.uuid4())
    mid2 = str(uuid.uuid4())

    _insert_memory(conn, mid1, "value A", slot="fact:test.key", scope_type="user")
    _insert_memory(conn, mid2, "value B", slot="fact:test.key", scope_type="user")

    # Retract the source memory
    now = datetime.utcnow().isoformat(timespec="seconds")
    conn.execute("UPDATE memories SET retracted_at = ? WHERE id = ?", (now, mid2))
    conn.commit()

    worker._process_conflicts(mid2)

    conflict_rows = conn.execute("SELECT * FROM memory_conflicts").fetchall()
    assert len(conflict_rows) == 0

    conn.close()
    writer.stop()
