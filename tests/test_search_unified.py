from __future__ import annotations

from pathlib import Path

from hoard.connectors.local_files.connector import LocalFilesConnector
from hoard.core.db.connection import connect, initialize_db
from hoard.core.ingest.sync import sync_connector
from hoard.core.memory.store import memory_put
from hoard.core.memory.v2.store import memory_write
from hoard.core.security.auth import TokenInfo
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


def test_search_respects_memory_sensitivity(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    config = {"vectors": {"enabled": False}, "search": {}, "write": {"embeddings": {"enabled": False}}}

    memory_write(
        conn,
        content="Restricted memory",
        memory_type="fact",
        scope_type="user",
        scope_id=None,
        source_agent="tester",
        sensitivity="restricted",
        config=config,
    )

    limited_agent = TokenInfo(
        name="limited",
        token=None,
        scopes={"memory.read"},
        capabilities={"memory.read"},
        trust_level=0.5,
        can_access_sensitive=False,
        can_access_restricted=False,
        requires_user_confirm=False,
        proposal_ttl_days=None,
        rate_limit_per_hour=0,
    )

    results, _ = search_entities(
        conn,
        query="Restricted",
        config=config,
        limit=5,
        types=["memory"],
        agent=limited_agent,
    )
    assert not results
    conn.close()
