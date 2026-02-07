from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from hoard.core.config import default_data_path, resolve_paths
from hoard.core.db.connection import initialize_db
from hoard.core.db.write_exec import WriteSubmit, direct_submit, temporary_coordinator_submit
from hoard.core.ingest.registry import iter_enabled_connectors
from hoard.core.ingest.sync import sync_connector_with_submit
from hoard.core.memory.store import memory_prune
from hoard.core.models import SyncStats


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
    if not _acquire_lock(lock_path):
        return {"skipped": True, "reason": "lock"}
    try:
        return sync_connectors(config, write_submit, source=source)
    finally:
        _release_lock(lock_path)


def acquire_sync_lock() -> bool:
    return _acquire_lock(_lock_path())


def release_sync_lock() -> None:
    _release_lock(_lock_path())


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


def _acquire_lock(path: Path) -> bool:
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        if _lock_is_stale(path):
            try:
                path.unlink()
            except Exception:
                return False
            return _acquire_lock(path)
        return False

    with os.fdopen(fd, "w") as handle:
        handle.write(f"{os.getpid()}\n{int(time.time())}\n")
    return True


def _release_lock(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return
    except Exception:
        return


def _lock_is_stale(path: Path) -> bool:
    try:
        content = path.read_text().splitlines()
        pid = int(content[0]) if content else None
    except Exception:
        return True

    if pid is None:
        return True
    return not _pid_alive(pid)


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False
