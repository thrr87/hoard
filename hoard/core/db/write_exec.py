from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Any, Callable, Generator, Mapping, Protocol

from hoard.core.db.writer import WriteCoordinator


class WriteSubmit(Protocol):
    def submit(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Execute a write closure with a writable DB connection."""


class _DirectWriteSubmit:
    def __init__(self, conn) -> None:
        self._conn = conn

    def submit(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        try:
            result = fn(self._conn, *args, **kwargs)
            if self._conn.in_transaction:
                self._conn.commit()
            return result
        except Exception:
            if self._conn.in_transaction:
                self._conn.rollback()
            raise


def direct_submit(conn) -> WriteSubmit:
    """Return a WriteSubmit implementation for an existing connection."""
    return _DirectWriteSubmit(conn)


@contextlib.contextmanager
def temporary_coordinator_submit(
    db_path: Path,
    db_cfg: Mapping[str, Any] | None = None,
) -> Generator[WriteSubmit, None, None]:
    """Create a temporary WriteCoordinator-backed submitter.

    Used by standalone flows (e.g. CLI sync) so they share the same
    lock/retry policy as the long-lived server writer.
    """
    cfg = db_cfg or {}
    writer = WriteCoordinator(
        db_path=db_path,
        busy_timeout_ms=_int_or_default(cfg.get("busy_timeout_ms"), 5000),
        lock_timeout_ms=_int_or_default(cfg.get("lock_timeout_ms"), 30000),
        retry_budget_ms=_int_or_default(cfg.get("retry_budget_ms"), 30000),
        retry_backoff_ms=min(_int_or_default(cfg.get("retry_backoff_ms"), 50), 500),
    )
    try:
        yield writer
    finally:
        writer.stop()


def _int_or_default(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default
