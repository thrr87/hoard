from __future__ import annotations

from typing import Iterable

from hoard.core.ingest.store import (
    get_entity_by_source,
    replace_chunks,
    tombstone_missing,
    upsert_entity,
)
from hoard.core.models import SyncStats
from hoard.core.ingest.registry import iter_enabled_connectors
from hoard.sdk.base import ConnectorV1
from hoard.sdk.types import EntityInput


def sync_connector(
    conn,
    connector: ConnectorV1,
    config: dict,
    on_entity: callable | None = None,
) -> SyncStats:
    stats = SyncStats()
    seen_source_ids: list[str] = []
    scan_failed = False

    try:
        for entity, chunks in connector.scan(config):
            stats.entities_seen += 1
            if on_entity:
                on_entity()

            try:
                _apply_provenance(connector, entity)
                existing = get_entity_by_source(conn, entity.source, entity.source_id)
                entity_id = upsert_entity(conn, entity)
                seen_source_ids.append(entity.source_id)

                if existing and existing.get("content_hash") == entity.content_hash:
                    continue

                stats.chunks_written += replace_chunks(conn, entity_id, chunks)
            except Exception:
                stats.errors += 1
                continue
    except Exception:
        stats.errors += 1
        scan_failed = True

    if not scan_failed:
        stats.entities_tombstoned = tombstone_missing(
            conn, connector.source_name, seen_source_ids
        )
    conn.commit()
    connector.cleanup()
    return stats


def _apply_provenance(connector: ConnectorV1, entity: EntityInput) -> None:
    if not entity.connector_name:
        entity.connector_name = connector.name
    if not entity.connector_version:
        entity.connector_version = connector.version
    if not entity.source:
        entity.source = connector.source_name


def run_sync(conn, *, config: dict, source: str | None = None) -> dict:
    results: dict[str, dict] = {}
    for name, connector, settings in iter_enabled_connectors(config):
        if source and name != source:
            continue
        stats = sync_connector(conn, connector, settings)
        results[name] = {
            "entities_seen": stats.entities_seen,
            "chunks_written": stats.chunks_written,
            "entities_tombstoned": stats.entities_tombstoned,
            "errors": stats.errors,
            "started_at": stats.started_at.isoformat(timespec="seconds"),
        }
    return results
