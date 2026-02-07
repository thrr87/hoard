from __future__ import annotations

import queue
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from hoard.core.db.connection import connect
from hoard.core.db.lock import DatabaseWriteLock


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

    def __init__(self, *, db_path: Path, busy_timeout_ms: int | None = None) -> None:
        self._queue: "queue.Queue[_WriteTask | None]" = queue.Queue()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._db_path = db_path
        self._busy_timeout_ms = busy_timeout_ms
        self._thread_id: int | None = None
        self._lock = DatabaseWriteLock(db_path)
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
            with self._lock:
                try:
                    task.result = task.fn(self._conn, *task.args, **task.kwargs)
                    if self._conn.in_transaction:
                        self._conn.commit()
                except Exception as exc:
                    if self._conn.in_transaction:
                        self._conn.rollback()
                    task.error = exc
                finally:
                    task.event.set()

        try:
            self._conn.close()
        except Exception:
            pass
