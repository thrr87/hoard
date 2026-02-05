from __future__ import annotations

import time
from pathlib import Path

import pytest

from hoard.core.sync.watcher import SyncWatcher, WatchTarget


def test_watcher_triggers_sync(tmp_path: Path) -> None:
    pytest.importorskip("watchdog")

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    target_file = data_dir / "note.md"
    target_file.write_text("initial")

    triggered = []

    def _on_sync(source: str) -> None:
        triggered.append(source)

    watcher = SyncWatcher(
        targets=[WatchTarget(source="local_files", path=data_dir)],
        on_sync=_on_sync,
        debounce_seconds=0.1,
    )
    watcher.start()
    try:
        target_file.write_text("updated")
        deadline = time.time() + 3
        while time.time() < deadline:
            if triggered:
                break
            time.sleep(0.05)
        assert triggered, "Expected watcher to trigger a sync callback"
    finally:
        watcher.stop()
