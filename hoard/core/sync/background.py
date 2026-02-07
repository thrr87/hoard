from __future__ import annotations

import threading
from pathlib import Path
from typing import Callable, Optional

from hoard.core.db.write_exec import WriteSubmit
from hoard.core.sync.service import run_sync_with_lock
from hoard.core.sync.watcher import WATCHDOG_AVAILABLE, SyncWatcher, build_watch_targets


class BackgroundSync:
    def __init__(
        self,
        *,
        config: dict,
        config_path: Optional[Path] = None,
        log: Optional[Callable[[str], None]] = None,
        write_submit: WriteSubmit | None = None,
    ) -> None:
        self._config = config
        self._config_path = config_path
        self._log = log or (lambda msg: None)
        self._write_submit = write_submit
        self._scheduler_thread: Optional[threading.Thread] = None
        self._watcher: Optional[SyncWatcher] = None
        self._stop = threading.Event()

    def start(self) -> None:
        interval = int(self._config.get("sync", {}).get("interval_minutes", 0) or 0)
        if interval > 0:
            self._scheduler_thread = threading.Thread(
                target=self._schedule_loop,
                args=(interval,),
                daemon=True,
            )
            self._scheduler_thread.start()
            self._log(f"Background sync every {interval} minutes enabled.")

        watcher_enabled = bool(self._config.get("sync", {}).get("watcher_enabled", False))
        if watcher_enabled:
            if not WATCHDOG_AVAILABLE:
                self._log("Watcher enabled but watchdog not installed. Skipping file watcher.")
                return

            debounce = float(self._config.get("sync", {}).get("watcher_debounce_seconds", 2) or 2)
            targets = build_watch_targets(self._config)
            if not targets:
                self._log("Watcher enabled but no valid watch targets found.")
                return

            self._watcher = SyncWatcher(
                targets=targets,
                on_sync=self._sync_source,
                debounce_seconds=debounce,
            )
            self._watcher.start()
            self._log("File watcher enabled.")

    def stop(self) -> None:
        self._stop.set()
        if self._watcher:
            self._watcher.stop()
            self._watcher = None
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            self._scheduler_thread.join(timeout=2)

    def _schedule_loop(self, interval_minutes: int) -> None:
        while not self._stop.wait(interval_minutes * 60):
            run_sync_with_lock(
                self._config,
                self._config_path,
                source=None,
                write_submit=self._write_submit,
            )

    def _sync_source(self, source: str) -> None:
        if self._stop.is_set():
            return
        run_sync_with_lock(
            self._config,
            self._config_path,
            source=source,
            write_submit=self._write_submit,
        )
