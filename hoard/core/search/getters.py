from __future__ import annotations

import json
from typing import Any, Dict, List, Optional


def get_entity(
    conn,
    entity_id: str,
    allow_sensitive: bool = True,
) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT * FROM entities
        WHERE id = ?
          AND tombstoned_at IS NULL
        """,
        (entity_id,),
    ).fetchone()

    if not row:
        return None

    if not allow_sensitive and row["sensitivity"] in {"sensitive", "secret"}:
        return None

    entity = _row_to_entity(row)
    chunks = conn.execute(
        """
        SELECT id, chunk_index, content, char_offset_start, char_offset_end, chunk_type
        FROM chunks
        WHERE entity_id = ?
        ORDER BY chunk_index ASC
        """,
        (entity_id,),
    ).fetchall()

    entity["chunks"] = [
        {
            "chunk_id": chunk["id"],
            "chunk_index": chunk["chunk_index"],
            "content": chunk["content"],
            "char_offset_start": chunk["char_offset_start"],
            "char_offset_end": chunk["char_offset_end"],
            "chunk_type": chunk["chunk_type"],
        }
        for chunk in chunks
    ]
    return entity


def get_chunk(
    conn,
    chunk_id: str,
    allow_sensitive: bool = True,
    context_chunks: int = 0,
) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT chunks.id AS chunk_id, chunks.entity_id, chunks.chunk_index,
               chunks.content, chunks.char_offset_start, chunks.char_offset_end,
               chunks.chunk_type, entities.title AS entity_title, entities.source,
               entities.uri, entities.sensitivity
        FROM chunks
        JOIN entities ON entities.id = chunks.entity_id
        WHERE chunks.id = ?
          AND entities.tombstoned_at IS NULL
        """,
        (chunk_id,),
    ).fetchone()

    if not row:
        return None

    if not allow_sensitive and row["sensitivity"] in {"sensitive", "secret"}:
        return None

    result = {
        "chunk_id": row["chunk_id"],
        "entity_id": row["entity_id"],
        "entity_title": row["entity_title"],
        "source": row["source"],
        "uri": row["uri"],
        "content": row["content"],
        "chunk_index": row["chunk_index"],
        "char_offset_start": row["char_offset_start"],
        "char_offset_end": row["char_offset_end"],
        "chunk_type": row["chunk_type"],
    }
    if context_chunks and context_chunks > 0:
        result["context"] = _fetch_context(conn, row["entity_id"], row["chunk_index"], context_chunks)
    return result


def _row_to_entity(row) -> Dict[str, Any]:
    tags = json.loads(row["tags"]) if row["tags"] else []
    metadata = json.loads(row["metadata"]) if row["metadata"] else None
    return {
        "id": row["id"],
        "source": row["source"],
        "source_id": row["source_id"],
        "entity_type": row["entity_type"],
        "title": row["title"],
        "uri": row["uri"],
        "mime_type": row["mime_type"],
        "tags": tags,
        "metadata": metadata,
        "sensitivity": row["sensitivity"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "synced_at": row["synced_at"],
        "last_seen_at": row["last_seen_at"],
        "tombstoned_at": row["tombstoned_at"],
        "content_hash": row["content_hash"],
        "connector_name": row["connector_name"],
        "connector_version": row["connector_version"],
    }


def _fetch_context(conn, entity_id: str, chunk_index: int, context_chunks: int) -> Dict[str, Any]:
    before_rows = conn.execute(
        """
        SELECT id, chunk_index, content, char_offset_start, char_offset_end, chunk_type
        FROM chunks
        WHERE entity_id = ? AND chunk_index < ?
        ORDER BY chunk_index DESC
        LIMIT ?
        """,
        (entity_id, chunk_index, context_chunks),
    ).fetchall()

    after_rows = conn.execute(
        """
        SELECT id, chunk_index, content, char_offset_start, char_offset_end, chunk_type
        FROM chunks
        WHERE entity_id = ? AND chunk_index > ?
        ORDER BY chunk_index ASC
        LIMIT ?
        """,
        (entity_id, chunk_index, context_chunks),
    ).fetchall()

    before = [
        {
            "chunk_id": row["id"],
            "chunk_index": row["chunk_index"],
            "content": row["content"],
            "char_offset_start": row["char_offset_start"],
            "char_offset_end": row["char_offset_end"],
            "chunk_type": row["chunk_type"],
        }
        for row in reversed(before_rows)
    ]

    after = [
        {
            "chunk_id": row["id"],
            "chunk_index": row["chunk_index"],
            "content": row["content"],
            "char_offset_start": row["char_offset_start"],
            "char_offset_end": row["char_offset_end"],
            "chunk_type": row["chunk_type"],
        }
        for row in after_rows
    ]

    return {"before": before, "after": after}
