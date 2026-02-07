"""Cross-process advisory locks for serialising database writes.

Uses ``fcntl.flock`` on dedicated lock files next to the database so that
**only one writer** (whether it lives inside an MCP server, a CLI command,
or a background-sync thread) can hold the write lock at any time.

Two lock files are used:

* ``<db>.lock``  -- **write lock**, held for the duration of each write
  transaction.  Prevents two processes from writing concurrently.
* ``<db>.server``  -- **server singleton lock**, held for the entire
  lifetime of a ``hoard serve`` process.  Prevents two servers from
  starting on the same database file.

Readers never need any lock (WAL mode guarantees non-blocking reads).
"""

from __future__ import annotations

import fcntl
import os
import time
from pathlib import Path
from types import TracebackType
from typing import Optional, Type


class DatabaseLockError(Exception):
    """Raised when a lock cannot be acquired."""


class _AdvisoryLock:
    """Low-level ``flock(2)``-based advisory lock on a file path."""

    def __init__(self, lock_path: Path, *, timeout_seconds: float = 30.0) -> None:
        self._lock_path = lock_path
        self._timeout = timeout_seconds
        self._fd: Optional[int] = None

    def __enter__(self) -> "_AdvisoryLock":
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.release()

    def acquire(self) -> None:
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = os.open(str(self._lock_path), os.O_CREAT | os.O_RDWR)

        deadline = time.monotonic() + self._timeout
        while True:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return
            except (BlockingIOError, OSError):
                if time.monotonic() >= deadline:
                    os.close(self._fd)
                    self._fd = None
                    raise DatabaseLockError(
                        f"Could not acquire lock {self._lock_path} within "
                        f"{self._timeout}s.  Another process may be holding it."
                    )
                time.sleep(0.05)

    def release(self) -> None:
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
            finally:
                os.close(self._fd)
                self._fd = None

    def try_acquire(self) -> bool:
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = os.open(str(self._lock_path), os.O_CREAT | os.O_RDWR)
        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except (BlockingIOError, OSError):
            os.close(self._fd)
            self._fd = None
            return False


class DatabaseWriteLock(_AdvisoryLock):
    """Exclusive, cross-process write lock.

    The lock file is ``<db_path>.lock`` (e.g. ``~/.hoard/hoard.db.lock``).

    Usage::

        lock = DatabaseWriteLock(db_path)
        with lock:
            conn.execute("INSERT ...")
            conn.commit()
    """

    def __init__(self, db_path: Path, *, timeout_seconds: float = 30.0) -> None:
        lock_path = db_path.with_suffix(db_path.suffix + ".lock")
        super().__init__(lock_path, timeout_seconds=timeout_seconds)


class ServerSingletonLock(_AdvisoryLock):
    """Prevents two ``hoard serve`` processes on the same database.

    The lock file is ``<db_path>.server`` and is held for the server's
    entire lifetime.  It does **not** conflict with ``DatabaseWriteLock``
    because it uses a different file.
    """

    def __init__(self, db_path: Path) -> None:
        lock_path = db_path.with_suffix(db_path.suffix + ".server")
        super().__init__(lock_path, timeout_seconds=0)

    def acquire_or_fail(self) -> None:
        """Acquire the lock or raise ``DatabaseLockError`` immediately."""
        if not self.try_acquire():
            raise DatabaseLockError(
                "Another hoard server is already running on this database.\n"
                "Only one server may write to a database at a time.\n"
                "Stop the other process first, or use a different storage.db_path."
            )
