from __future__ import annotations

from pathlib import Path

from hoard.core.db.connection import connect, initialize_db
from hoard.core.sync.service import sync_with_lock


def test_sync_with_lock_runs(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "a.md").write_text("sync test")

    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

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
        "memory": {"prune_on_sync": False},
    }

    result = sync_with_lock(conn, config, lock_path=tmp_path / "sync.lock")
    assert "connectors" in result
    assert result["connectors"]
    conn.close()
