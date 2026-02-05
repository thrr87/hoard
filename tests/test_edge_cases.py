from __future__ import annotations

import os
from pathlib import Path

import pytest

from hoard.core.ingest.inbox import write_inbox_entry
from hoard.core.memory.store import MemoryError, memory_get, memory_put
from hoard.core.sync.service import sync_with_lock
from hoard.core.db.connection import connect, initialize_db


def test_inbox_write_collision(tmp_path: Path) -> None:
    inbox_dir = tmp_path / "inbox"
    inbox_dir.mkdir()
    config = {
        "connectors": {
            "inbox": {
                "enabled": True,
                "path": str(inbox_dir),
                "include_extensions": [".md"],
            }
        }
    }

    first = write_inbox_entry(config, content="Hello", title="Same Title")
    second = write_inbox_entry(config, content="Hello", title="Same Title")

    assert first.exists()
    assert second.exists()
    assert first.name != second.name


def test_memory_ttl_zero_expires_immediately(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    entry = memory_put(conn, key="immediate", content="expired", ttl_days=0)
    assert entry["expires_at"] is not None
    assert memory_get(conn, "immediate") is None
    conn.close()


def test_memory_ttl_invalid_raises(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    with pytest.raises(MemoryError):
        memory_put(conn, key="bad", content="bad", ttl_days="nope")  # type: ignore[arg-type]
    conn.close()


def test_sync_lock_contention(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    lock_path = tmp_path / "sync.lock"
    lock_path.write_text(f"{os.getpid()}\n0\n")

    result = sync_with_lock(conn, {"connectors": {}}, lock_path=lock_path)
    assert result.get("skipped") is True
    assert result.get("reason") == "lock"
    conn.close()
