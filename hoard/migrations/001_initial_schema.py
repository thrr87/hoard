from __future__ import annotations

from hoard.core.db.schema import apply_schema

VERSION = 1


def up(conn) -> None:
    """Create initial schema."""
    apply_schema(conn)


def down(conn) -> None:
    raise NotImplementedError("Rollback not supported for this migration")
