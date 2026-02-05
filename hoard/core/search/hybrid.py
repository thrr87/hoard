from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from hoard.core.embeddings.model import EmbeddingError, EmbeddingModel
from hoard.core.search.bm25 import search_chunks_flat
from hoard.core.search.vector import vector_search

_model_cache: dict[str, EmbeddingModel] = {}


def _get_model(model_name: str) -> EmbeddingModel:
    if model_name not in _model_cache:
        _model_cache[model_name] = EmbeddingModel(model_name)
    return _model_cache[model_name]


def hybrid_search(
    conn,
    *,
    query: str,
    config: dict,
    limit: int = 20,
    source: str | None = None,
    allow_sensitive: bool = True,
) -> List[Dict[str, Any]]:
    if not query.strip():
        return []

    search_config = config.get("search", {})
    vectors_config = config.get("vectors", {})

    rrf_k = int(search_config.get("rrf_k", 60))
    max_chunks_per_entity = int(search_config.get("max_chunks_per_entity", 3))

    vectors_enabled = bool(vectors_config.get("enabled", False))
    model_name = vectors_config.get("model_name", "sentence-transformers/all-MiniLM-L6-v2")
    prefilter_limit = int(vectors_config.get("prefilter_limit", 1000))

    total_chunks = _count_chunks(conn, source, allow_sensitive)
    use_prefilter = vectors_enabled and total_chunks > 50_000

    bm25_limit = prefilter_limit if use_prefilter else max(limit * 20, 200)
    bm25_results = search_chunks_flat(
        conn,
        query,
        limit=bm25_limit,
        offset=0,
        source=source,
        allow_sensitive=allow_sensitive,
    )
    bm25_rank = {row["chunk_id"]: idx + 1 for idx, row in enumerate(bm25_results)}
    bm25_scores = {row["chunk_id"]: row["score"] for row in bm25_results}

    candidate_ids = [row["chunk_id"] for row in bm25_results] if use_prefilter else None

    vector_rank: dict[str, int] = {}
    vector_scores: dict[str, float] = {}

    if vectors_enabled:
        try:
            model = _get_model(model_name)
            query_vector = model.encode([query])[0]
            vector_results = vector_search(
                conn,
                query_vector=query_vector,
                model_name=model_name,
                limit=bm25_limit,
                candidate_chunk_ids=candidate_ids,
                source=source,
                allow_sensitive=allow_sensitive,
            )
            vector_rank = {row["chunk_id"]: idx + 1 for idx, row in enumerate(vector_results)}
            vector_scores = {row["chunk_id"]: row["score"] for row in vector_results}
        except EmbeddingError:
            vectors_enabled = False

    scores: dict[str, float] = {}
    for chunk_id in set(bm25_rank) | set(vector_rank):
        score = 0.0
        if chunk_id in bm25_rank:
            score += 1.0 / (rrf_k + bm25_rank[chunk_id])
        if chunk_id in vector_rank:
            score += 1.0 / (rrf_k + vector_rank[chunk_id])
        scores[chunk_id] = score

    sorted_chunks = sorted(scores.items(), key=lambda item: item[1], reverse=True)

    chunk_ids = [chunk_id for chunk_id, _ in sorted_chunks]
    details_map = _fetch_chunk_details(conn, chunk_ids)

    grouped: dict[str, Dict[str, Any]] = {}
    for chunk_id, score in sorted_chunks:
        detail = details_map.get(chunk_id)
        if not detail:
            continue

        entity_id = detail["entity_id"]
        if entity_id not in grouped:
            grouped[entity_id] = {
                "entity_id": entity_id,
                "entity_title": detail["entity_title"],
                "source": detail["source"],
                "uri": detail["uri"],
                "entity_updated_at": detail["entity_updated_at"],
                "chunks": [],
            }

        if len(grouped[entity_id]["chunks"]) >= max_chunks_per_entity:
            continue

        grouped[entity_id]["chunks"].append(
            {
                "chunk_id": detail["chunk_id"],
                "content": detail["content"],
                "score": score,
                "bm25_score": bm25_scores.get(chunk_id),
                "vector_score": vector_scores.get(chunk_id),
                "char_offset_start": detail["char_offset_start"],
                "char_offset_end": detail["char_offset_end"],
            }
        )

        if len(grouped) >= limit:
            if all(
                len(entry["chunks"]) >= max_chunks_per_entity for entry in grouped.values()
            ):
                break

    return list(grouped.values())[:limit]


def _count_chunks(conn, source: str | None, allow_sensitive: bool) -> int:
    filters = ["entities.tombstoned_at IS NULL"]
    params: List[Any] = []
    if source:
        filters.append("entities.source = ?")
        params.append(source)
    if not allow_sensitive:
        filters.append("entities.sensitivity NOT IN ('sensitive', 'secret')")

    where_clause = " AND ".join(filters)
    row = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM chunks
        JOIN entities ON entities.id = chunks.entity_id
        WHERE {where_clause}
        """,
        params,
    ).fetchone()
    return int(row[0]) if row else 0


def _fetch_chunk_details(conn, chunk_ids: Iterable[str]) -> Dict[str, Dict[str, Any]]:
    ids = list(chunk_ids)
    if not ids:
        return {}

    placeholders = ",".join("?" for _ in ids)
    rows = conn.execute(
        f"""
        SELECT chunks.id AS chunk_id, chunks.entity_id, chunks.content,
               chunks.char_offset_start, chunks.char_offset_end,
               entities.title AS entity_title, entities.source, entities.uri,
               entities.updated_at AS entity_updated_at
        FROM chunks
        JOIN entities ON entities.id = chunks.entity_id
        WHERE chunks.id IN ({placeholders})
        """,
        ids,
    ).fetchall()

    return {
        row["chunk_id"]: {
            "chunk_id": row["chunk_id"],
            "entity_id": row["entity_id"],
            "content": row["content"],
            "char_offset_start": row["char_offset_start"],
            "char_offset_end": row["char_offset_end"],
            "entity_title": row["entity_title"],
            "source": row["source"],
            "uri": row["uri"],
            "entity_updated_at": row["entity_updated_at"],
        }
        for row in rows
    }
