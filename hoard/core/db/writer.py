from __future__ import annotations

import queue
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from hoard.core.db.connection import connect
from hoard.core.db.lock import DatabaseLockError, DatabaseWriteLock


@dataclass
class _WriteTask:
    fn: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    event: threading.Event
    result: Any = None
    error: Exception | None = None


class WriteCoordinator:
    """Serialises all database writes through a single thread **and** holds a
    cross-process ``flock``-based advisory lock so that concurrent processes
    (CLI commands, background sync, a second server) are also serialised.
    """

    def __init__(
        self,
        *,
        db_path: Path,
        busy_timeout_ms: int | None = None,
        lock_timeout_ms: int | None = None,
        retry_budget_ms: int | None = None,
        retry_backoff_ms: int | None = None,
    ) -> None:
        self._queue: "queue.Queue[_WriteTask | None]" = queue.Queue()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._db_path = db_path
        self._busy_timeout_ms = busy_timeout_ms
        self._lock_timeout_ms = _int_or_default(lock_timeout_ms, 30000)
        self._retry_budget_ms = _int_or_default(retry_budget_ms, 30000)
        self._retry_backoff_ms = min(_int_or_default(retry_backoff_ms, 50), 500)
        self._thread_id: int | None = None
        self._lock = DatabaseWriteLock(
            db_path,
            timeout_seconds=self._lock_timeout_ms / 1000,
        )
        self._thread.start()

    def submit(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        if threading.get_ident() == self._thread_id:
            return fn(self._conn, *args, **kwargs)

        task = _WriteTask(fn=fn, args=args, kwargs=kwargs, event=threading.Event())
        self._queue.put(task)
        task.event.wait()
        if task.error:
            raise task.error
        return task.result

    def stop(self) -> None:
        self._queue.put(None)
        self._thread.join(timeout=5)

    def _run(self) -> None:
        self._thread_id = threading.get_ident()
        self._conn = connect(self._db_path, busy_timeout_ms=self._busy_timeout_ms)
        while True:
            task = self._queue.get()
            if task is None:
                break
            try:
                self._run_task_with_retry(task)
            except Exception as exc:
                # Defensive safety net: report task failures and keep
                # the writer loop alive for subsequent tasks.
                task.error = exc
            finally:
                task.event.set()

        try:
            self._conn.close()
        except Exception:
            pass

    def _run_task_with_retry(self, task: _WriteTask) -> None:
        deadline = time.monotonic() + (self._retry_budget_ms / 1000)
        while True:
            try:
                with self._lock:
                    try:
                        task.result = task.fn(self._conn, *task.args, **task.kwargs)
                        if self._conn.in_transaction:
                            self._conn.commit()
                    except Exception as exc:
                        if self._conn.in_transaction:
                            self._conn.rollback()
                        task.error = exc
                    return
            except Exception as exc:
                if not self._is_retryable(exc):
                    task.error = exc
                    return
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    task.error = exc
                    return
                time.sleep(min(self._retry_backoff_ms / 1000, remaining))

    @staticmethod
    def _is_retryable(exc: Exception) -> bool:
        if isinstance(exc, DatabaseLockError):
            return True
        if isinstance(exc, sqlite3.OperationalError):
            msg = str(exc).lower()
            if "busy" in msg or "locked" in msg:
                return True
        return False


def _int_or_default(value: int | None, default: int) -> int:
    if value is None:
        return default
    return value if value > 0 else default
