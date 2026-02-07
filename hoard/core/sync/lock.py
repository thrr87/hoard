from __future__ import annotations

import fcntl
from pathlib import Path


class SyncFileLock:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._fd = None

    def acquire(self, *, blocking: bool = False) -> bool:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = self._path.open("a+")
        flags = fcntl.LOCK_EX
        if not blocking:
            flags |= fcntl.LOCK_NB
        try:
            fcntl.flock(self._fd.fileno(), flags)
            return True
        except BlockingIOError:
            self._fd.close()
            self._fd = None
            return False

    def release(self) -> None:
        if self._fd is None:
            return
        try:
            fcntl.flock(self._fd.fileno(), fcntl.LOCK_UN)
        finally:
            self._fd.close()
            self._fd = None

