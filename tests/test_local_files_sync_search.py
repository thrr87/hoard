from __future__ import annotations

from pathlib import Path

from hoard.connectors.local_files.connector import LocalFilesConnector
from hoard.core.db.connection import connect, initialize_db
from hoard.core.ingest.sync import sync_connector
from hoard.core.search.bm25 import search_chunks


def test_local_files_sync_and_search(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    file_path = data_dir / "notes.md"
    file_path.write_text("Hello world. This is a Hoard test.")

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
    assert stats.chunks_written >= 1

    results = search_chunks(conn, "Hoard test")
    assert results

    conn.close()
