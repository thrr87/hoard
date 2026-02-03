from __future__ import annotations

from pathlib import Path

from hoard.connectors.local_files.connector import LocalFilesConnector
from hoard.core.db.connection import connect, initialize_db
from hoard.core.ingest.sync import sync_connector
from hoard.core.search.hybrid import hybrid_search


def test_hybrid_search_without_vectors(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    file_path = data_dir / "notes.md"
    file_path.write_text("Hybrid search should find this content.")

    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    connector = LocalFilesConnector()
    config = {
        "paths": [str(data_dir)],
        "include_extensions": [".md"],
        "chunk_max_tokens": 50,
        "chunk_overlap_tokens": 0,
    }

    stats = sync_connector(conn, connector, config)
    assert stats.entities_seen == 1

    results = hybrid_search(
        conn,
        query="Hybrid search",
        config={"vectors": {"enabled": False}, "search": {}},
        limit=5,
    )
    assert results
    conn.close()
