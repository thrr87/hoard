from __future__ import annotations

VERSION = 8


def up(conn) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS ann_index_meta (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            backend TEXT NOT NULL,
            model_name TEXT NOT NULL,
            vectors_count INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'stale'
        );

        INSERT OR IGNORE INTO ann_index_meta (id, backend, model_name, vectors_count, updated_at, state)
        VALUES (1, 'hnsw', '', 0, datetime('now'), 'stale');
        """
    )


def down(conn) -> None:
    conn.execute("DROP TABLE IF EXISTS ann_index_meta")

