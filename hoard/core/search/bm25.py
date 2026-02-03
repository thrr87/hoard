from __future__ import annotations

from typing import Any, Dict, List


def search_chunks_flat(
    conn,
    query: str,
    limit: int = 20,
    offset: int = 0,
    source: str | None = None,
    allow_sensitive: bool = True,
) -> List[Dict[str, Any]]:
    if not query.strip():
        return []

    sql = (
        "SELECT chunks.id AS chunk_id, chunks.entity_id AS entity_id, chunks.content AS content, "
        "chunks.char_offset_start AS char_offset_start, chunks.char_offset_end AS char_offset_end, "
        "entities.title AS entity_title, entities.source AS source, entities.uri AS uri, "
        "-bm25(chunks_fts) AS score "
        "FROM chunks_fts "
        "JOIN chunks ON chunks_fts.rowid = chunks.rowid "
        "JOIN entities ON entities.id = chunks.entity_id "
        "WHERE chunks_fts MATCH ?"
        " AND entities.tombstoned_at IS NULL"
    )

    params: list[Any] = [query]
    if source:
        sql += " AND entities.source = ?"
        params.append(source)
    if not allow_sensitive:
        sql += " AND entities.sensitivity NOT IN ('sensitive', 'secret')"

    sql += " ORDER BY score DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    rows = conn.execute(sql, params).fetchall()
    return [
        {
            "chunk_id": row["chunk_id"],
            "entity_id": row["entity_id"],
            "content": row["content"],
            "score": row["score"],
            "char_offset_start": row["char_offset_start"],
            "char_offset_end": row["char_offset_end"],
            "entity_title": row["entity_title"],
            "source": row["source"],
            "uri": row["uri"],
        }
        for row in rows
    ]


def search_chunks(
    conn,
    query: str,
    limit: int = 20,
    source: str | None = None,
    allow_sensitive: bool = True,
) -> List[Dict[str, Any]]:
    rows = search_chunks_flat(
        conn,
        query,
        limit=limit,
        offset=0,
        source=source,
        allow_sensitive=allow_sensitive,
    )
    grouped: dict[str, Dict[str, Any]] = {}

    for row in rows:
        entity_id = row["entity_id"]
        if entity_id not in grouped:
            grouped[entity_id] = {
                "entity_id": entity_id,
                "entity_title": row["entity_title"],
                "source": row["source"],
                "uri": row["uri"],
                "chunks": [],
            }
        grouped[entity_id]["chunks"].append(
            {
                "chunk_id": row["chunk_id"],
                "content": row["content"],
                "score": row["score"],
                "char_offset_start": row["char_offset_start"],
                "char_offset_end": row["char_offset_end"],
            }
        )

    return list(grouped.values())


def search_entities_bm25(
    conn,
    query: str,
    limit: int = 20,
    offset: int = 0,
    source: str | None = None,
    allow_sensitive: bool = True,
    max_chunks_per_entity: int = 3,
) -> tuple[List[Dict[str, Any]], str | None]:
    if limit < 0:
        limit = 0
    if offset < 0:
        offset = 0

    fetch_limit = (limit + offset + 1) * max(1, max_chunks_per_entity)
    rows = search_chunks_flat(
        conn,
        query,
        limit=fetch_limit,
        offset=0,
        source=source,
        allow_sensitive=allow_sensitive,
    )
    grouped: dict[str, Dict[str, Any]] = {}

    for row in rows:
        entity_id = row["entity_id"]
        if entity_id not in grouped:
            grouped[entity_id] = {
                "entity_id": entity_id,
                "entity_title": row["entity_title"],
                "source": row["source"],
                "uri": row["uri"],
                "chunks": [],
            }
        if len(grouped[entity_id]["chunks"]) >= max_chunks_per_entity:
            continue
        grouped[entity_id]["chunks"].append(
            {
                "chunk_id": row["chunk_id"],
                "content": row["content"],
                "score": row["score"],
                "char_offset_start": row["char_offset_start"],
                "char_offset_end": row["char_offset_end"],
            }
        )

    entities = list(grouped.values())
    sliced = entities[offset : offset + limit]
    has_more = len(entities) > offset + limit
    next_cursor = str(offset + limit) if has_more else None
    return sliced, next_cursor
