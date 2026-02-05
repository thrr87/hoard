from __future__ import annotations

import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional

try:
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer

    WATCHDOG_AVAILABLE = True
except ImportError:
    FileSystemEventHandler = object  # type: ignore
    Observer = None  # type: ignore
    WATCHDOG_AVAILABLE = False


@dataclass
class WatchTarget:
    source: str
    path: Path
    target_file: Optional[Path] = None


class SyncWatcher:
    def __init__(
        self,
        targets: Iterable[WatchTarget],
        on_sync: Callable[[str], None],
        debounce_seconds: float = 2.0,
    ) -> None:
        if not WATCHDOG_AVAILABLE:
            raise RuntimeError("watchdog is not installed")
        self._observer = Observer()
        self._targets = list(targets)
        self._on_sync = on_sync
        self._debounce_seconds = debounce_seconds
        self._timers: Dict[str, threading.Timer] = {}

    def start(self) -> None:
        for target in self._targets:
            handler = _SourceHandler(
                source=target.source,
                schedule=self._schedule_sync,
                target_file=target.target_file,
            )
            recursive = target.target_file is None and target.path.is_dir()
            self._observer.schedule(handler, str(target.path), recursive=recursive)
        self._observer.start()

    def stop(self) -> None:
        self._observer.stop()
        self._observer.join(timeout=2)

    def _schedule_sync(self, source: str) -> None:
        existing = self._timers.get(source)
        if existing:
            existing.cancel()

        timer = threading.Timer(self._debounce_seconds, self._on_sync, args=(source,))
        timer.daemon = True
        self._timers[source] = timer
        timer.start()


class _SourceHandler(FileSystemEventHandler):
    def __init__(
        self,
        *,
        source: str,
        schedule: Callable[[str], None],
        target_file: Optional[Path] = None,
    ) -> None:
        self._source = source
        self._schedule = schedule
        self._target_file = target_file

    def on_any_event(self, event) -> None:
        if self._target_file:
            if Path(event.src_path) != self._target_file:
                return
        self._schedule(self._source)


def build_watch_targets(config: dict) -> List[WatchTarget]:
    targets: List[WatchTarget] = []
    connectors = config.get("connectors", {})

    local_files = connectors.get("local_files", {})
    if local_files.get("enabled", False):
        for path in local_files.get("paths", []) or []:
            _add_target(targets, "local_files", Path(path))

    inbox = connectors.get("inbox", {})
    if inbox.get("enabled", False) and inbox.get("path"):
        _add_target(targets, "inbox", Path(inbox.get("path")))

    obsidian = connectors.get("obsidian", {})
    if obsidian.get("enabled", False) and obsidian.get("vault_path"):
        _add_target(targets, "obsidian", Path(obsidian.get("vault_path")))

    notion = connectors.get("notion_export", {})
    if notion.get("enabled", False) and notion.get("export_path"):
        _add_target(targets, "notion_export", Path(notion.get("export_path")))

    chrome = connectors.get("bookmarks_chrome", {})
    if chrome.get("enabled", False) and chrome.get("bookmark_path"):
        _add_target(targets, "bookmarks_chrome", Path(chrome.get("bookmark_path")))

    firefox = connectors.get("bookmarks_firefox", {})
    if firefox.get("enabled", False) and firefox.get("places_path"):
        _add_target(targets, "bookmarks_firefox", Path(firefox.get("places_path")))

    return targets


def _add_target(targets: List[WatchTarget], source: str, path: Path) -> None:
    expanded = path.expanduser()
    if not expanded.exists():
        return

    if expanded.is_file():
        targets.append(WatchTarget(source=source, path=expanded.parent, target_file=expanded))
    else:
        targets.append(WatchTarget(source=source, path=expanded))
