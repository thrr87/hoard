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
