from __future__ import annotations

import json
from datetime import datetime
from typing import Iterable, List, Optional

from hoard.core.db.connection import executemany
from hoard.core.ingest.hash import compute_content_hash
from hoard.sdk.types import ChunkInput, EntityInput


def build_entity_id(source: str, source_id: str) -> str:
    return compute_content_hash(f"{source}:{source_id}")


def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def get_entity_by_source(conn, source: str, source_id: str) -> Optional[dict]:
    row = conn.execute(
        "SELECT * FROM entities WHERE source = ? AND source_id = ?",
        (source, source_id),
    ).fetchone()
    return dict(row) if row else None


def upsert_entity(conn, entity: EntityInput) -> str:
    entity_id = build_entity_id(entity.source, entity.source_id)
    now = _now_iso()

    tags = entity.tags or []
    tags_text = " ".join(tags)

    metadata_json = json.dumps(entity.metadata) if entity.metadata else None
    tags_json = json.dumps(tags) if tags else None

    created_at = entity.created_at.isoformat(timespec="seconds") if entity.created_at else now
    updated_at = entity.updated_at.isoformat(timespec="seconds") if entity.updated_at else now

    conn.execute(
        """
        INSERT INTO entities (
            id, source, source_id, entity_type, title, uri, mime_type,
            tags, tags_text, metadata, sensitivity, created_at, updated_at,
            synced_at, last_seen_at, tombstoned_at, content_hash,
            connector_name, connector_version
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)
        ON CONFLICT(source, source_id) DO UPDATE SET
            entity_type = excluded.entity_type,
            title = excluded.title,
            uri = excluded.uri,
            mime_type = excluded.mime_type,
            tags = excluded.tags,
            tags_text = excluded.tags_text,
            metadata = excluded.metadata,
            sensitivity = excluded.sensitivity,
            updated_at = excluded.updated_at,
            synced_at = excluded.synced_at,
            last_seen_at = excluded.last_seen_at,
            tombstoned_at = NULL,
            content_hash = excluded.content_hash,
            connector_name = excluded.connector_name,
            connector_version = excluded.connector_version
        """,
        (
            entity_id,
            entity.source,
            entity.source_id,
            entity.entity_type,
            entity.title,
            entity.uri,
            entity.mime_type,
            tags_json,
            tags_text,
            metadata_json,
            entity.sensitivity,
            created_at,
            updated_at,
            now,
            now,
            entity.content_hash,
            entity.connector_name,
            entity.connector_version,
        ),
    )
    return entity_id


def replace_chunks(conn, entity_id: str, chunks: List[ChunkInput]) -> int:
    conn.execute("DELETE FROM chunks WHERE entity_id = ?", (entity_id,))

    rows = []
    for index, chunk in enumerate(chunks):
        chunk_id = f"{entity_id}:{index}"
        rows.append(
            (
                chunk_id,
                entity_id,
                index,
                chunk.content,
                compute_content_hash(chunk.content),
                chunk.char_offset_start,
                chunk.char_offset_end,
                chunk.chunk_type,
            )
        )

    executemany(
        conn,
        """
        INSERT INTO chunks (
            id, entity_id, chunk_index, content, content_hash,
            char_offset_start, char_offset_end, chunk_type
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def tombstone_missing(conn, source: str, seen_source_ids: Iterable[str]) -> int:
    now = _now_iso()
    seen_list = list(seen_source_ids)
    if not seen_list:
        cursor = conn.execute(
            "UPDATE entities SET tombstoned_at = ?, last_seen_at = NULL WHERE source = ?",
            (now, source),
        )
        return cursor.rowcount

    conn.execute("CREATE TEMP TABLE IF NOT EXISTS _hoard_seen_source_ids (source_id TEXT PRIMARY KEY)")
    conn.execute("DELETE FROM _hoard_seen_source_ids")
    conn.executemany(
        "INSERT OR IGNORE INTO _hoard_seen_source_ids (source_id) VALUES (?)",
        [(source_id,) for source_id in seen_list],
    )

    cursor = conn.execute(
        """
        UPDATE entities
        SET tombstoned_at = ?, last_seen_at = NULL
        WHERE source = ?
          AND source_id NOT IN (SELECT source_id FROM _hoard_seen_source_ids)
        """,
        (now, source),
    )
    conn.execute("DROP TABLE IF EXISTS _hoard_seen_source_ids")
    return cursor.rowcount
