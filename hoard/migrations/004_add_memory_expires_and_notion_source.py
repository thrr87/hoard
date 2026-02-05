from __future__ import annotations

import sqlite3

VERSION = 4


def _table_exists(conn, name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (name,),
    ).fetchone()
    return row is not None


def _column_exists(conn, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    for row in rows:
        name = row["name"] if isinstance(row, sqlite3.Row) else row[1]
        if name == column:
            return True
    return False


def up(conn) -> None:
    if _table_exists(conn, "memory_entries") and not _column_exists(conn, "memory_entries", "expires_at"):
        conn.execute("ALTER TABLE memory_entries ADD COLUMN expires_at DATETIME")

    conn.execute(
        """
        UPDATE entities
        SET source = 'notion_export'
        WHERE source = 'notion' OR connector_name = 'notion_export'
        """
    )


def down(conn) -> None:
    raise NotImplementedError("Rollback not supported for this migration")
