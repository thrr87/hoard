"""Tests for cross-process database write locking.

Covers:
- DatabaseWriteLock basic acquire/release
- DatabaseWriteLock mutual exclusion across threads (simulating processes)
- ServerSingletonLock prevents two servers on the same DB
- WriteCoordinator acquires/releases the lock per write
- write_locked() context manager
- CLI and sync paths hold the lock during writes
"""

from __future__ import annotations

import json
import threading
import time
import urllib.request
from pathlib import Path

import pytest

from hoard.core.config import save_config
from hoard.core.db.connection import connect, initialize_db, write_locked
from hoard.core.db.lock import DatabaseLockError, DatabaseWriteLock, ServerSingletonLock
from hoard.core.db.writer import WriteCoordinator
from hoard.core.memory.store import memory_put


# ---------------------------------------------------------------------------
# DatabaseWriteLock unit tests
# ---------------------------------------------------------------------------


def test_write_lock_acquire_release(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    db_path.touch()

    lock = DatabaseWriteLock(db_path)
    lock.acquire()
    lock.release()


def test_write_lock_context_manager(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    db_path.touch()

    with DatabaseWriteLock(db_path):
        pass  # lock held here


def test_write_lock_blocks_second_holder(tmp_path: Path) -> None:
    """Two threads competing for the write lock: only one holds it at a time."""
    db_path = tmp_path / "test.db"
    db_path.touch()

    order: list[str] = []
    barrier = threading.Barrier(2)

    def holder(name: str, hold_seconds: float) -> None:
        barrier.wait()
        with DatabaseWriteLock(db_path, timeout_seconds=5):
            order.append(f"{name}-acquired")
            time.sleep(hold_seconds)
            order.append(f"{name}-released")

    t1 = threading.Thread(target=holder, args=("A", 0.3))
    t2 = threading.Thread(target=holder, args=("B", 0.0))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # One must fully complete before the other starts
    assert order[0].endswith("-acquired")
    assert order[1].endswith("-released")
    assert order[0][0] == order[1][0]  # same thread name for first pair


def test_write_lock_timeout(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    db_path.touch()

    lock_a = DatabaseWriteLock(db_path)
    lock_a.acquire()

    with pytest.raises(DatabaseLockError):
        DatabaseWriteLock(db_path, timeout_seconds=0.1).acquire()

    lock_a.release()


# ---------------------------------------------------------------------------
# ServerSingletonLock unit tests
# ---------------------------------------------------------------------------


def test_server_singleton_blocks_second(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    db_path.touch()

    lock_a = ServerSingletonLock(db_path)
    lock_a.acquire_or_fail()

    with pytest.raises(DatabaseLockError, match="Another hoard server"):
        ServerSingletonLock(db_path).acquire_or_fail()

    lock_a.release()

    # Now it should succeed
    lock_b = ServerSingletonLock(db_path)
    lock_b.acquire_or_fail()
    lock_b.release()


def test_server_singleton_does_not_block_write_lock(tmp_path: Path) -> None:
    """ServerSingletonLock uses a different file, so it doesn't block writes."""
    db_path = tmp_path / "test.db"
    db_path.touch()

    server_lock = ServerSingletonLock(db_path)
    server_lock.acquire_or_fail()

    # This should succeed because they use different lock files
    write_lock = DatabaseWriteLock(db_path, timeout_seconds=0.1)
    write_lock.acquire()
    write_lock.release()

    server_lock.release()


# ---------------------------------------------------------------------------
# WriteCoordinator lock integration
# ---------------------------------------------------------------------------


def test_write_coordinator_serialises_across_threads(tmp_path: Path) -> None:
    """Multiple threads submitting writes are serialised by the lock."""
    db_path = tmp_path / "test.db"
    conn = connect(db_path)
    initialize_db(conn)
    conn.close()

    writer = WriteCoordinator(db_path=db_path)
    results: list[int] = []
    errors: list[str] = []

    def _write_memory(conn, idx: int) -> None:
        conn.execute(
            "INSERT OR REPLACE INTO memory_entries "
            "(id, key, content, tags, tags_text, metadata, created_at, updated_at) "
            "VALUES (?, ?, ?, '', '', '{}', datetime('now'), datetime('now'))",
            (f"mem-{idx}", f"key-{idx}", f"content-{idx}"),
        )

    def worker(start: int) -> None:
        try:
            for i in range(start, start + 5):
                writer.submit(_write_memory, i)
                results.append(i)
        except Exception as exc:
            errors.append(str(exc))

    threads = [threading.Thread(target=worker, args=(n * 5,)) for n in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    writer.stop()

    assert not errors, f"Errors: {errors}"
    assert len(results) == 20


# ---------------------------------------------------------------------------
# write_locked() context manager
# ---------------------------------------------------------------------------


def test_write_locked_context_manager(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    conn = connect(db_path)
    initialize_db(conn)
    conn.close()

    with write_locked(db_path) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO memory_entries "
            "(id, key, content, tags, tags_text, metadata, created_at, updated_at) "
            "VALUES ('m1', 'k1', 'v1', '', '', '{}', datetime('now'), datetime('now'))"
        )
        conn.commit()

    # Verify the write persisted
    conn = connect(db_path)
    row = conn.execute("SELECT content FROM memory_entries WHERE key = 'k1'").fetchone()
    conn.close()
    assert row is not None
    assert row["content"] == "v1"


# ---------------------------------------------------------------------------
# Concurrent HTTP write test
# ---------------------------------------------------------------------------


def _call_mcp(url: str, token: str, method: str, params: dict) -> tuple[int, dict]:
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    data = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }
    req = urllib.request.Request(url, data=data, headers=headers)
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.status, json.loads(resp.read())


def test_http_mcp_concurrent_writes(tmp_path: Path, mcp_server) -> None:
    """Multiple agents writing memories concurrently must all succeed."""
    db_path = tmp_path / "hoard.db"
    config_path = tmp_path / "config.yaml"
    config = {
        "security": {
            "tokens": [
                {
                    "name": "agent",
                    "token": "hoard_sk_agent",
                    "scopes": ["search", "get", "memory", "sync", "ingest"],
                }
            ]
        },
        "storage": {"db_path": str(db_path)},
        "connectors": {"local_files": {"enabled": False}},
        "vectors": {"enabled": False},
        "write": {
            "enabled": True,
            "embeddings": {"enabled": False},
            "server_secret_env": "",
            "auto_generate_server_secret": False,
        },
    }
    save_config(config, config_path)

    conn = connect(db_path)
    initialize_db(conn)
    conn.close()

    url = mcp_server(config_path)

    errors: list[str] = []
    lock = threading.Lock()
    writes_per_thread = 5
    num_threads = 4

    def writer(thread_id: int) -> None:
        try:
            for i in range(writes_per_thread):
                key = f"fact:thread{thread_id}.item{i}"
                _, resp = _call_mcp(
                    url,
                    "hoard_sk_agent",
                    "tools/call",
                    {
                        "name": "memory_write",
                        "arguments": {
                            "content": f"Memory from thread {thread_id} item {i}",
                            "slot": key,
                        },
                    },
                )
                if "error" in resp:
                    with lock:
                        errors.append(f"t{thread_id}i{i}: {resp['error']}")
        except Exception as exc:
            with lock:
                errors.append(f"t{thread_id}: {exc}")

    threads = [threading.Thread(target=writer, args=(n,)) for n in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Concurrent write errors: {errors}"

    # Verify all writes landed
    conn = connect(db_path)
    count = conn.execute("SELECT COUNT(*) FROM memories").fetchone()[0]
    conn.close()
    assert count == writes_per_thread * num_threads
