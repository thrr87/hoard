from __future__ import annotations

from pathlib import Path

from hoard.connectors.local_files.connector import LocalFilesConnector
from hoard.core.db.connection import connect, initialize_db
from hoard.core.ingest.sync import sync_connector
from hoard.core.memory.store import memory_put
from hoard.core.search.service import search_entities


def test_search_unified_memory_and_entities(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "notes.md").write_text("Hoard document content")

    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    connector = LocalFilesConnector()
    config = {
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
        "search": {},
    }

    sync_connector(conn, connector, config["connectors"]["local_files"])
    memory_put(conn, key="context", content="Hoard memory entry")

    results, _ = search_entities(conn, query="Hoard", config=config, limit=10, types=["entity", "memory"])
    result_types = {entry.get("result_type") for entry in results}
    assert "entity" in result_types
    assert "memory" in result_types

    memory_only, _ = search_entities(conn, query="Hoard", config=config, limit=10, types=["memory"])
    assert memory_only
    assert all(entry.get("result_type") == "memory" for entry in memory_only)

    conn.close()
