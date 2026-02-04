from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

from hoard import __version__
from hoard.migrations import migrate


def connect(db_path: Path, *, busy_timeout_ms: int | None = None) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _apply_pragmas(conn, busy_timeout_ms=busy_timeout_ms)
    return conn


def _apply_pragmas(conn: sqlite3.Connection, *, busy_timeout_ms: int | None = None) -> None:
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    if busy_timeout_ms:
        conn.execute(f"PRAGMA busy_timeout = {int(busy_timeout_ms)};")


def initialize_db(conn: sqlite3.Connection, app_version: str | None = None) -> None:
    migrate(conn, app_version=app_version or __version__)


def executemany(conn: sqlite3.Connection, sql: str, rows: Iterable[tuple]) -> None:
    conn.executemany(sql, rows)


def ensure_sqlite_version(min_version: tuple[int, int, int] = (3, 35, 0)) -> None:
    if sqlite3.sqlite_version_info < min_version:
        raise RuntimeError(
            f"SQLite {min_version} required, found {sqlite3.sqlite_version}"
        )
