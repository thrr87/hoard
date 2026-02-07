from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

from hoard.core.config import default_data_path, resolve_paths
from hoard.core.db.connection import initialize_db
from hoard.core.db.write_exec import WriteSubmit, direct_submit, temporary_coordinator_submit
from hoard.core.ingest.registry import iter_enabled_connectors
from hoard.core.ingest.sync import sync_connector_with_submit
from hoard.core.memory.store import memory_prune
from hoard.core.models import SyncStats
from hoard.core.sync.lock import SyncFileLock


_GLOBAL_SYNC_LOCK: SyncFileLock | None = None


def sync_connectors(
    config: dict,
    write_submit: WriteSubmit,
    source: Optional[str] = None,
    on_entity: Optional[callable] = None,
) -> Dict[str, Any]:
    results: List[Dict[str, Any]] = []

    for name, connector, settings in iter_enabled_connectors(config):
        if source and source not in {name, connector.source_name}:
            continue

        discover = connector.discover(settings)
        if not discover.success:
            results.append(
                {
                    "source": name,
                    "success": False,
                    "message": discover.message,
                    "stats": None,
                }
            )
            continue

        stats = sync_connector_with_submit(
            connector,
            settings,
            submit_write=write_submit,
            on_entity=on_entity,
        )
        results.append(
            {
                "source": name,
                "success": True,
                "message": discover.message,
                "stats": _stats_to_dict(stats),
            }
        )

    pruned = 0
    if config.get("memory", {}).get("prune_on_sync", True):
        pruned = write_submit.submit(memory_prune)

    return {"connectors": results, "memory_pruned": pruned}


def run_sync_with_lock(
    config: dict,
    config_path: Optional[Path] = None,
    source: Optional[str] = None,
    lock_path: Optional[Path] = None,
    write_submit: WriteSubmit | None = None,
) -> Dict[str, Any]:
    """Run a sync, acquiring the file-based sync lock to prevent overlaps."""
    if write_submit is not None:
        write_submit.submit(initialize_db)
        return _sync_with_lock_submit(
            config,
            write_submit=write_submit,
            source=source,
            lock_path=lock_path,
        )

    paths = resolve_paths(config, config_path)
    db_cfg = config.get("write", {}).get("database", {})
    with temporary_coordinator_submit(paths.db_path, db_cfg) as temp_submit:
        temp_submit.submit(initialize_db)
        return _sync_with_lock_submit(
            config,
            write_submit=temp_submit,
            source=source,
            lock_path=lock_path,
        )


def sync_with_lock(
    conn,
    config: dict,
    source: Optional[str] = None,
    lock_path: Optional[Path] = None,
) -> Dict[str, Any]:
    return _sync_with_lock_submit(
        config,
        write_submit=direct_submit(conn),
        source=source,
        lock_path=lock_path,
    )


def _sync_with_lock_submit(
    config: dict,
    write_submit: WriteSubmit,
    source: Optional[str] = None,
    lock_path: Optional[Path] = None,
) -> Dict[str, Any]:
    lock_path = lock_path or _lock_path()
    lock = _acquire_lock(lock_path)
    if lock is None:
        return {"skipped": True, "reason": "lock"}
    try:
        return sync_connectors(config, write_submit, source=source)
    finally:
        _release_lock(lock)


def acquire_sync_lock() -> bool:
    global _GLOBAL_SYNC_LOCK
    if _GLOBAL_SYNC_LOCK is not None:
        return True
    lock = _acquire_lock(_lock_path())
    if lock is None:
        return False
    _GLOBAL_SYNC_LOCK = lock
    return True


def release_sync_lock() -> None:
    global _GLOBAL_SYNC_LOCK
    if _GLOBAL_SYNC_LOCK is None:
        return
    _release_lock(_GLOBAL_SYNC_LOCK)
    _GLOBAL_SYNC_LOCK = None


def _stats_to_dict(stats: SyncStats) -> Dict[str, Any]:
    return {
        "entities_seen": stats.entities_seen,
        "chunks_written": stats.chunks_written,
        "entities_tombstoned": stats.entities_tombstoned,
        "errors": stats.errors,
        "started_at": stats.started_at.isoformat(timespec="seconds"),
    }


def _lock_path() -> Path:
    base = default_data_path()
    try:
        base.mkdir(parents=True, exist_ok=True)
    except PermissionError:
        base = Path(tempfile.gettempdir()) / "hoard"
        base.mkdir(parents=True, exist_ok=True)
        return base / "sync.lock"

    if not os.access(base, os.W_OK):
        base = Path(tempfile.gettempdir()) / "hoard"
        base.mkdir(parents=True, exist_ok=True)

    return base / "sync.lock"


def _acquire_lock(path: Path) -> SyncFileLock | None:
    lock = SyncFileLock(path)
    if not lock.acquire(blocking=False):
        return None
    return lock


def _release_lock(lock: SyncFileLock) -> None:
    lock.release()
