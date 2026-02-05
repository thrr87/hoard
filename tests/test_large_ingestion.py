from __future__ import annotations

from pathlib import Path

from hoard.connectors.local_files.connector import LocalFilesConnector
from hoard.core.db.connection import connect, initialize_db
from hoard.core.ingest.sync import sync_connector


def test_large_ingestion_sync(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    for idx in range(200):
        (data_dir / f"file_{idx}.md").write_text(f"Document {idx} with content")

    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    config = {
        "enabled": True,
        "paths": [str(data_dir)],
        "include_extensions": [".md"],
        "chunk_max_tokens": 50,
        "chunk_overlap_tokens": 0,
    }

    stats = sync_connector(conn, LocalFilesConnector(), config)
    assert stats.entities_seen == 200
    assert stats.chunks_written >= 200
    conn.close()
