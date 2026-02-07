"""Tests for cross-process database write locking.

Covers:
- DatabaseWriteLock basic acquire/release
- DatabaseWriteLock mutual exclusion across threads (simulating processes)
- ServerSingletonLock prevents two servers on the same DB
- WriteCoordinator acquires/releases the lock per write
- WriteCoordinator resilience when lock is contended
- WriteCoordinator retries under transient contention
- WriteCoordinator fails deterministically after retry budget
- write_locked() context manager
- Background sync + MCP writes serialize without lock errors
- CLI sync and live server writes serialize safely
- Server singleton lifecycle releases lock on shutdown
- Transaction ownership via submit helpers
"""

from __future__ import annotations

import json
import socket
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest
from click.testing import CliRunner

from hoard.cli.main import cli
from hoard.core.config import save_config
from hoard.core.db.connection import connect, initialize_db, write_locked
from hoard.core.db.lock import DatabaseLockError, DatabaseWriteLock, ServerSingletonLock
from hoard.core.db.write_exec import direct_submit
from hoard.core.db.writer import WriteCoordinator
from hoard.core.mcp.server import run_server
from hoard.core.sync.background import BackgroundSync

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


def test_write_coordinator_survives_lock_contention(tmp_path: Path) -> None:
    """If another thread holds the flock, the WriteCoordinator must report
    the error to the caller and keep running -- not deadlock or crash.

    Regression test: previously, a DatabaseLockError from acquire() would
    propagate past the task loop, killing the writer thread and leaving
    all submit() callers hanging on event.wait() forever.
    """
    db_path = tmp_path / "test.db"
    conn = connect(db_path)
    initialize_db(conn)
    conn.close()

    writer = WriteCoordinator(db_path=db_path)

    # Hold the flock from another fd (simulating BackgroundSync or CLI)
    external_lock = DatabaseWriteLock(db_path)
    external_lock.acquire()

    # Submit a write -- should fail with DatabaseLockError, NOT hang
    def _noop_write(conn):
        pass

    with pytest.raises(Exception):
        writer.submit(_noop_write)

    # Release the external lock
    external_lock.release()

    # Writer thread must still be alive -- a subsequent write should succeed
    def _insert(conn):
        conn.execute(
            "INSERT OR REPLACE INTO memory_entries "
            "(id, key, content, tags, tags_text, metadata, created_at, updated_at) "
            "VALUES ('m1', 'k1', 'survived', '', '', '{}', datetime('now'), datetime('now'))"
        )

    writer.submit(_insert)
    writer.stop()

    conn = connect(db_path)
    row = conn.execute("SELECT content FROM memory_entries WHERE key = 'k1'").fetchone()
    conn.close()
    assert row is not None
    assert row["content"] == "survived"


def test_write_coordinator_retries_and_succeeds(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    conn = connect(db_path)
    initialize_db(conn)
    conn.close()

    writer = WriteCoordinator(
        db_path=db_path,
        lock_timeout_ms=100,
        retry_budget_ms=1500,
        retry_backoff_ms=50,
    )
    external_lock = DatabaseWriteLock(db_path, timeout_seconds=5)
    external_lock.acquire()

    def _release_later() -> None:
        time.sleep(0.3)
        external_lock.release()

    releaser = threading.Thread(target=_release_later)
    releaser.start()

    def _insert(conn):
        conn.execute(
            "INSERT OR REPLACE INTO memory_entries "
            "(id, key, content, tags, tags_text, metadata, created_at, updated_at) "
            "VALUES ('m2', 'k2', 'retried', '', '', '{}', datetime('now'), datetime('now'))"
        )

    writer.submit(_insert)
    releaser.join(timeout=2)
    writer.stop()

    conn = connect(db_path)
    row = conn.execute("SELECT content FROM memory_entries WHERE key = 'k2'").fetchone()
    conn.close()
    assert row is not None
    assert row["content"] == "retried"


def test_write_coordinator_retry_budget_exhausted(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    conn = connect(db_path)
    initialize_db(conn)
    conn.close()

    writer = WriteCoordinator(
        db_path=db_path,
        lock_timeout_ms=50,
        retry_budget_ms=150,
        retry_backoff_ms=25,
    )
    external_lock = DatabaseWriteLock(db_path, timeout_seconds=5)
    external_lock.acquire()

    with pytest.raises(DatabaseLockError):
        writer.submit(lambda conn: None)

    external_lock.release()
    writer.stop()


def test_direct_submit_commits_memory_put(tmp_path: Path) -> None:
    from hoard.core.memory.store import memory_get, memory_put

    db_path = tmp_path / "test.db"
    conn = connect(db_path)
    initialize_db(conn)
    submitter = direct_submit(conn)

    submitter.submit(memory_put, key="ctx", content="value")
    conn.close()

    conn = connect(db_path)
    row = memory_get(conn, "ctx")
    conn.close()
    assert row is not None
    assert row["content"] == "value"


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


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_health(port: int, timeout_seconds: float = 10) -> None:
    deadline = time.time() + timeout_seconds
    url = f"http://127.0.0.1:{port}/health"
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as resp:
                if resp.status in {200, 503}:
                    return
        except Exception:
            time.sleep(0.05)
    raise AssertionError(f"Server did not become healthy on port {port}")


def _start_run_server(
    *,
    config_path: Path,
    port: int,
) -> tuple[threading.Event, threading.Thread, list[Exception]]:
    stop_event = threading.Event()
    errors: list[Exception] = []

    def _target() -> None:
        try:
            run_server(
                host="127.0.0.1",
                port=port,
                config_path=config_path,
                stop_event=stop_event,
            )
        except Exception as exc:  # pragma: no cover - diagnostic path
            errors.append(exc)

    thread = threading.Thread(target=_target, daemon=True)
    thread.start()
    return stop_event, thread, errors


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


def test_background_sync_and_http_writes_do_not_lock(
    tmp_path: Path,
    monkeypatch,
    mcp_server_with_instance,
) -> None:
    monkeypatch.setenv("HOARD_DATA_DIR", str(tmp_path / "hoard-data"))

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    for idx in range(40):
        (data_dir / f"note-{idx}.md").write_text(f"content {idx}")

    db_path = tmp_path / "hoard.db"
    secret_path = tmp_path / "server.key"
    secret_path.write_text("test-secret\n")
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
        "connectors": {
            "local_files": {
                "enabled": True,
                "paths": [str(data_dir)],
                "include_extensions": [".md"],
                "chunk_max_tokens": 50,
                "chunk_overlap_tokens": 0,
            }
        },
        "vectors": {"enabled": False},
        "write": {
            "enabled": True,
            "server_secret_file": str(secret_path),
            "auto_generate_server_secret": False,
            "embeddings": {"enabled": False},
            "database": {
                "busy_timeout_ms": 5000,
                "lock_timeout_ms": 5000,
                "retry_budget_ms": 5000,
                "retry_backoff_ms": 25,
            },
        },
    }
    save_config(config, config_path)

    conn = connect(db_path)
    initialize_db(conn)
    conn.close()

    url, server = mcp_server_with_instance(config_path)
    bg = BackgroundSync(
        config=config,
        config_path=config_path,
        write_submit=server.writer,
    )

    sync_thread = threading.Thread(target=bg._sync_source, args=("local_files",), daemon=True)
    sync_thread.start()

    errors: list[str] = []
    lock = threading.Lock()

    def _write_worker() -> None:
        for idx in range(30):
            try:
                _, resp = _call_mcp(
                    url,
                    "hoard_sk_agent",
                    "tools/call",
                    {
                        "name": "memory_write",
                        "arguments": {
                            "content": f"from writer {idx}",
                            "slot": f"fact:bg.{idx}",
                        },
                    },
                )
                if "error" in resp:
                    with lock:
                        errors.append(str(resp["error"]))
            except Exception as exc:  # pragma: no cover - diagnostic
                with lock:
                    errors.append(str(exc))

    write_threads = [threading.Thread(target=_write_worker, daemon=True) for _ in range(2)]
    for thread in write_threads:
        thread.start()
    for thread in write_threads:
        thread.join()
    sync_thread.join(timeout=20)

    assert not [err for err in errors if "locked" in err.lower()], f"Unexpected lock errors: {errors}"


def test_cli_sync_and_live_server_writes_serialize(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("HOARD_DATA_DIR", str(tmp_path / "hoard-data"))

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    for idx in range(20):
        (data_dir / f"doc-{idx}.md").write_text(f"sync doc {idx}")

    db_path = tmp_path / "hoard.db"
    secret_path = tmp_path / "server.key"
    secret_path.write_text("test-secret\n")
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
        "connectors": {
            "local_files": {
                "enabled": True,
                "paths": [str(data_dir)],
                "include_extensions": [".md"],
                "chunk_max_tokens": 50,
                "chunk_overlap_tokens": 0,
            }
        },
        "vectors": {"enabled": False},
        "write": {
            "enabled": True,
            "server_secret_file": str(secret_path),
            "auto_generate_server_secret": False,
            "embeddings": {"enabled": False},
            "database": {
                "busy_timeout_ms": 5000,
                "lock_timeout_ms": 5000,
                "retry_budget_ms": 5000,
                "retry_backoff_ms": 25,
            },
        },
    }
    save_config(config, config_path)

    conn = connect(db_path)
    initialize_db(conn)
    conn.close()

    port = _free_port()
    stop_event, server_thread, server_errors = _start_run_server(config_path=config_path, port=port)
    _wait_for_health(port)
    url = f"http://127.0.0.1:{port}/mcp"

    write_errors: list[str] = []
    write_lock = threading.Lock()

    def _server_writer() -> None:
        for idx in range(25):
            try:
                _, resp = _call_mcp(
                    url,
                    "hoard_sk_agent",
                    "tools/call",
                    {
                        "name": "memory_write",
                        "arguments": {
                            "content": f"cli-race {idx}",
                            "slot": f"fact:cli.{idx}",
                        },
                    },
                )
                if "error" in resp:
                    with write_lock:
                        write_errors.append(str(resp["error"]))
            except Exception as exc:  # pragma: no cover - diagnostic
                with write_lock:
                    write_errors.append(str(exc))

    writer_thread = threading.Thread(target=_server_writer, daemon=True)
    writer_thread.start()

    runner = CliRunner()
    sync_result = runner.invoke(cli, ["sync", "--config", str(config_path)])

    writer_thread.join(timeout=20)
    stop_event.set()
    server_thread.join(timeout=5)

    assert sync_result.exit_code == 0, sync_result.output
    assert not server_errors, f"Server errors: {server_errors}"
    assert not [err for err in write_errors if "locked" in err.lower()], write_errors


def test_server_singleton_lock_released_after_shutdown(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("HOARD_DATA_DIR", str(tmp_path / "hoard-data"))

    db_path = tmp_path / "hoard.db"
    secret_path = tmp_path / "server.key"
    secret_path.write_text("test-secret\n")
    config_path = tmp_path / "config.yaml"
    config = {
        "security": {"tokens": []},
        "storage": {"db_path": str(db_path)},
        "connectors": {"local_files": {"enabled": False}},
        "vectors": {"enabled": False},
        "write": {
            "enabled": True,
            "server_secret_file": str(secret_path),
            "auto_generate_server_secret": False,
            "embeddings": {"enabled": False},
        },
    }
    save_config(config, config_path)

    conn = connect(db_path)
    initialize_db(conn)
    conn.close()

    port_a = _free_port()
    stop_a, thread_a, errors_a = _start_run_server(config_path=config_path, port=port_a)
    _wait_for_health(port_a)

    with pytest.raises(DatabaseLockError):
        run_server(host="127.0.0.1", port=_free_port(), config_path=config_path, stop_event=threading.Event())

    stop_a.set()
    thread_a.join(timeout=5)
    assert not errors_a

    port_b = _free_port()
    stop_b, thread_b, errors_b = _start_run_server(config_path=config_path, port=port_b)
    _wait_for_health(port_b)
    stop_b.set()
    thread_b.join(timeout=5)
    assert not errors_b
