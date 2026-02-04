from __future__ import annotations

import multiprocessing
import sqlite3
from pathlib import Path

import pytest

from hoard.migrations import MigrationError, get_current_version, get_migrations, migrate


def _migrate_process(db_path: str, result_queue: multiprocessing.Queue) -> None:
    conn = sqlite3.connect(db_path)
    try:
        applied = migrate(conn)
        result_queue.put(("success", applied))
    except MigrationError as exc:
        result_queue.put(("migration_error", str(exc)))
    except Exception as exc:  # pragma: no cover - unexpected
        result_queue.put(("error", str(exc)))
    finally:
        conn.close()


def test_fresh_install(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)

    applied = migrate(conn)

    assert applied
    assert get_current_version(conn) == max(applied)
    conn.close()


def test_incremental_upgrade(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)

    migrate(conn, target_version=1)
    assert get_current_version(conn) == 1

    migrate(conn)
    latest = max(get_migrations().keys()) if get_migrations() else 0
    assert get_current_version(conn) == latest
    conn.close()


def test_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)

    migrate(conn)
    version_after_first = get_current_version(conn)

    applied = migrate(conn)
    assert applied == []
    assert get_current_version(conn) == version_after_first
    conn.close()


def test_migration_failure_rollback(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    migrate(conn)
    good_version = get_current_version(conn)

    good_history_count = conn.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0]
    conn.close()

    import hoard.migrations as migrations

    original_get_migrations = migrations.get_migrations

    class FailingMigration:
        __name__ = "999_failing_migration"
        VERSION = good_version + 1

        @staticmethod
        def up(conn) -> None:
            conn.execute("CREATE TABLE temp_test (id TEXT)")
            raise RuntimeError("Intentional failure for testing")

    def patched_get_migrations():
        result = original_get_migrations()
        result[good_version + 1] = FailingMigration
        return result

    migrations.get_migrations = patched_get_migrations

    try:
        conn = sqlite3.connect(db_path)
        with pytest.raises(MigrationError) as exc:
            migrate(conn)
        assert "Intentional failure" in str(exc.value)

        assert get_current_version(conn) == good_version
        new_history_count = conn.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0]
        assert new_history_count == good_history_count

        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='temp_test'"
        ).fetchall()
        assert len(tables) == 0, "Partial migration changes should be rolled back"
        conn.close()
    finally:
        migrations.get_migrations = original_get_migrations


def test_downgrade_detection(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA user_version = 9999")

    with pytest.raises(MigrationError) as exc:
        migrate(conn)
    assert "newer than code" in str(exc.value)
    conn.close()


def test_concurrent_access(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"

    ctx = multiprocessing.get_context("spawn")
    result_queue: multiprocessing.Queue = ctx.Queue()

    p1 = ctx.Process(target=_migrate_process, args=(str(db_path), result_queue))
    p2 = ctx.Process(target=_migrate_process, args=(str(db_path), result_queue))
    p1.start()
    p2.start()
    p1.join(timeout=30)
    p2.join(timeout=30)
    if p1.is_alive():
        p1.terminate()
        p1.join(timeout=5)
    if p2.is_alive():
        p2.terminate()
        p2.join(timeout=5)

    results = []
    while not result_queue.empty():
        results.append(result_queue.get())

    successes = [r for r in results if r[0] == "success"]
    assert len(successes) >= 1

    errors = [r for r in results if r[0] == "migration_error"]
    for _, msg in errors:
        assert "locked" in msg.lower() or "no pending" in msg.lower()

    conn = sqlite3.connect(db_path)
    assert get_current_version(conn) > 0
    conn.close()


def test_history_recorded(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)

    migrate(conn, app_version="1.0.0")
    rows = conn.execute("SELECT * FROM schema_migrations").fetchall()
    assert rows
    assert rows[0][3] == "1.0.0"
    assert rows[0][5] is not None
    conn.close()


def test_checksum_stored(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)

    migrate(conn)
    rows = conn.execute(
        "SELECT version, checksum FROM schema_migrations WHERE checksum IS NOT NULL"
    ).fetchall()
    assert rows
    for _, checksum in rows:
        assert len(checksum) == 16
    conn.close()
