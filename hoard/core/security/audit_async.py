from __future__ import annotations

import queue
import threading
from pathlib import Path
from typing import Any

from hoard.core.db.connection import connect
from hoard.core.security.audit import log_access


class AsyncAuditSink:
    def __init__(self, *, db_path: Path, queue_size: int = 10000) -> None:
        self._db_path = db_path
        self._queue: "queue.Queue[dict[str, Any] | None]" = queue.Queue(maxsize=max(1, queue_size))
        self._dropped = 0
        self._dropped_lock = threading.Lock()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def submit(
        self,
        *,
        tool: str,
        success: bool,
        token_name: str | None,
        scope: str | None = None,
        chunks_returned: int = 0,
        bytes_returned: int = 0,
    ) -> bool:
        item = {
            "tool": tool,
            "success": success,
            "token_name": token_name,
            "scope": scope,
            "chunks_returned": chunks_returned,
            "bytes_returned": bytes_returned,
        }
        try:
            self._queue.put_nowait(item)
            return True
        except queue.Full:
            with self._dropped_lock:
                self._dropped += 1
            return False

    def dropped_count(self) -> int:
        with self._dropped_lock:
            return self._dropped

    def queue_depth(self) -> int:
        return self._queue.qsize()

    def stop(self) -> None:
        self._queue.put(None)
        self._thread.join(timeout=5)

    def _run(self) -> None:
        conn = connect(self._db_path)
        try:
            while True:
                item = self._queue.get()
                if item is None:
                    break
                try:
                    log_access(conn, update_rate_limits=False, **item)
                except Exception:
                    continue
        finally:
            conn.close()
