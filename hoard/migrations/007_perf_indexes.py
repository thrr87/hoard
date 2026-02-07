from __future__ import annotations

VERSION = 7


def up(conn) -> None:
    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_entities_tombstoned_at
            ON entities(tombstoned_at);
        CREATE INDEX IF NOT EXISTS idx_entities_source
            ON entities(source);
        CREATE INDEX IF NOT EXISTS idx_entities_sensitivity
            ON entities(sensitivity);
        """
    )


def down(conn) -> None:
    conn.executescript(
        """
        DROP INDEX IF EXISTS idx_entities_tombstoned_at;
        DROP INDEX IF EXISTS idx_entities_source;
        DROP INDEX IF EXISTS idx_entities_sensitivity;
        """
    )

