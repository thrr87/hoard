from __future__ import annotations

from pathlib import Path

from hoard.core.db.connection import connect, initialize_db
from hoard.core.memory.store import memory_get, memory_put, memory_search


def test_memory_put_get_search(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    entry = memory_put(conn, key="project", content="Hoard memory entry", tags=["test"])
    assert entry["key"] == "project"

    fetched = memory_get(conn, "project")
    assert fetched is not None
    assert fetched["content"] == "Hoard memory entry"

    results = memory_search(conn, "Hoard")
    assert results
    conn.close()


def test_memory_ttl_expiration(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    entry = memory_put(conn, key="ttl", content="expires soon", ttl_days=1)
    assert entry["expires_at"] is not None

    expired_entry = memory_put(
        conn,
        key="expired",
        content="expired content",
        expires_at="2000-01-01T00:00:00",
    )
    assert expired_entry["expires_at"] == "2000-01-01T00:00:00"

    assert memory_get(conn, "ttl") is not None
    assert memory_get(conn, "expired") is None

    results = memory_search(conn, "expired")
    assert not results
    conn.close()
