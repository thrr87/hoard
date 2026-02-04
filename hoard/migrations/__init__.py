from __future__ import annotations

import hashlib
import importlib
import inspect
import pkgutil
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import click


class MigrationError(Exception):
    pass


def compute_checksum(migration_module) -> str:
    """Compute SHA-256 of migration source for drift detection."""
    try:
        source = inspect.getsource(migration_module)
    except OSError:
        return ""
    return hashlib.sha256(source.encode()).hexdigest()[:16]


def get_migrations() -> Dict[int, object]:
    """Discover all migrations, sorted by version."""
    migrations: Dict[int, object] = {}
    package_path = Path(__file__).parent

    for _, name, _ in pkgutil.iter_modules([str(package_path)]):
        if name.startswith("_"):
            continue
        module = importlib.import_module(f".{name}", __package__)
        if not hasattr(module, "VERSION") or not hasattr(module, "up"):
            continue

        version = getattr(module, "VERSION")
        if not isinstance(version, int):
            raise MigrationError(f"Migration {name}: VERSION must be int, got {type(version)}")

        if version in migrations:
            raise MigrationError(
                f"Duplicate migration version {version}: "
                f"{migrations[version].__name__} and {module.__name__}"
            )

        migrations[version] = module

    return dict(sorted(migrations.items()))


def validate_migration_sequence(migrations: Dict[int, object], current: int, target: int) -> None:
    """Ensure no gaps in migration sequence."""
    for version in range(current + 1, target + 1):
        if version not in migrations:
            available = sorted(migrations.keys())
            raise MigrationError(
                f"Missing migration for version {version}. Available versions: {available}"
            )


def get_current_version(conn: sqlite3.Connection) -> int:
    """Get current schema version from DB."""
    return int(conn.execute("PRAGMA user_version").fetchone()[0])


def ensure_history_table(conn: sqlite3.Connection) -> None:
    """Create schema_migrations table if not exists."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at TEXT NOT NULL,
            app_version TEXT,
            duration_ms INTEGER,
            checksum TEXT
        )
        """
    )


def record_migration(
    conn: sqlite3.Connection,
    version: int,
    name: str,
    duration_ms: int,
    app_version: str | None = None,
    checksum: str | None = None,
) -> None:
    """Record migration in history table."""
    conn.execute(
        """
        INSERT OR REPLACE INTO schema_migrations
        (version, name, applied_at, app_version, duration_ms, checksum)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (version, name, datetime.utcnow().isoformat(), app_version, duration_ms, checksum),
    )


def check_migration_integrity(
    conn: sqlite3.Connection, migrations: Dict[int, object], warn: bool = True
) -> List[Tuple[int, str, str, str]]:
    """Return mismatches between applied migrations and current code."""
    try:
        rows = conn.execute(
            "SELECT version, name, checksum FROM schema_migrations WHERE checksum IS NOT NULL"
        ).fetchall()
    except sqlite3.OperationalError:
        return []

    mismatches: List[Tuple[int, str, str, str]] = []
    for row in rows:
        version, name, stored_checksum = row[0], row[1], row[2]
        if version in migrations and stored_checksum:
            current_checksum = compute_checksum(migrations[version])
            if current_checksum and stored_checksum != current_checksum:
                mismatches.append((version, name, stored_checksum, current_checksum))
                if warn:
                    click.echo(
                        f"⚠️  Migration {version} ({name}) checksum mismatch!\n"
                        f"   DB was migrated by different code. Proceeding may be risky.\n"
                        f"   Stored: {stored_checksum}, Current: {current_checksum}",
                        err=True,
                    )
    return mismatches


def get_pending_versions(conn: sqlite3.Connection, target_version: int | None = None) -> List[int]:
    migrations = get_migrations()
    if not migrations:
        return []
    current = get_current_version(conn)
    target = target_version or max(migrations.keys())
    if current >= target:
        return []
    validate_migration_sequence(migrations, current, target)
    return list(range(current + 1, target + 1))


def _begin_immediate(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError as exc:
        if "locked" in str(exc).lower():
            raise MigrationError(
                "Database is locked (another process may be migrating). "
                "Wait a moment and try again, or run 'hoard db status' to check."
            ) from exc
        raise


def migrate(
    conn: sqlite3.Connection, target_version: int | None = None, app_version: str | None = None
) -> List[int]:
    """
    Run all pending migrations up to target_version.
    If target_version is None, migrate to latest.
    Returns list of applied version numbers.
    """
    migrations = get_migrations()

    if not migrations:
        return []

    current = get_current_version(conn)
    latest = max(migrations.keys())
    if current > latest:
        raise MigrationError(
            f"Database version ({current}) is newer than code ({latest}). "
            "Did you downgrade Hoard? Options:\n"
            "  1. Upgrade Hoard to the version that created this DB\n"
            "  2. Run 'hoard db reset' to rebuild (loses memory data)\n"
            "  3. Restore from backup"
        )

    target = target_version or latest
    if current >= target:
        return []

    validate_migration_sequence(migrations, current, target)

    conn.execute("PRAGMA busy_timeout = 10000")
    conn.execute("PRAGMA foreign_keys = ON")

    ensure_history_table(conn)
    conn.commit()

    check_migration_integrity(conn, migrations, warn=True)

    applied: List[int] = []
    _begin_immediate(conn)

    current = get_current_version(conn)
    if current >= target:
        conn.commit()
        return []

    for version in range(current + 1, target + 1):
        migration = migrations[version]
        migration_name = migration.__name__.split(".")[-1]
        checksum = compute_checksum(migration)

        try:
            start_time = time.perf_counter()
            migration.up(conn)
            duration_ms = int((time.perf_counter() - start_time) * 1000)

            conn.execute(f"PRAGMA user_version = {version}")
            record_migration(conn, version, migration_name, duration_ms, app_version, checksum)
            conn.commit()
            applied.append(version)

            if version < target:
                _begin_immediate(conn)
        except Exception as exc:
            conn.rollback()
            raise MigrationError(
                f"Migration {version} ({migration_name}) failed: {exc}\n"
                f"Database left at version {get_current_version(conn)}"
            ) from exc

    if applied:
        fk_errors = conn.execute("PRAGMA foreign_key_check").fetchall()
        if fk_errors:
            click.echo(
                "⚠️  Foreign key violations detected after migrations!\n"
                "   Run 'hoard db repair' to fix, or check migration logic.\n"
                f"   Violations: {fk_errors[:5]}{'...' if len(fk_errors) > 5 else ''}",
                err=True,
            )

    return applied
