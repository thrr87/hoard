from __future__ import annotations

from pathlib import Path

from hoard.core.db.connection import connect, initialize_db
from hoard.core.ingest.store import tombstone_missing


def test_tombstone_missing_large_source(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    source = "local_files"
    total = 1200
    seen = 1000

    rows = [
        (f"id-{idx}", source, f"/path/{idx}.md", "document")
        for idx in range(total)
    ]
    conn.executemany(
        "INSERT INTO entities (id, source, source_id, entity_type) VALUES (?, ?, ?, ?)",
        rows,
    )
    conn.commit()

    seen_ids = [f"/path/{idx}.md" for idx in range(seen)]
    tombstone_missing(conn, source, seen_ids)

    count = conn.execute(
        "SELECT COUNT(*) FROM entities WHERE tombstoned_at IS NOT NULL"
    ).fetchone()[0]
    assert count == total - seen

    conn.close()
