from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from hoard.core.embeddings.store import deserialize_vector


def vector_search(
    conn,
    *,
    query_vector: List[float],
    model_name: str,
    limit: int = 20,
    candidate_chunk_ids: Optional[Iterable[str]] = None,
    source: str | None = None,
    allow_sensitive: bool = True,
) -> List[Dict[str, Any]]:
    if not query_vector:
        return []

    params: List[Any] = [model_name]
    filters = ["embeddings.model = ?", "entities.tombstoned_at IS NULL"]

    if source:
        filters.append("entities.source = ?")
        params.append(source)

    if not allow_sensitive:
        filters.append("entities.sensitivity NOT IN ('sensitive', 'secret')")

    if candidate_chunk_ids is not None:
        ids = list(candidate_chunk_ids)
        if not ids:
            return []
        placeholders = ",".join("?" for _ in ids)
        filters.append(f"embeddings.chunk_id IN ({placeholders})")
        params.extend(ids)

    where_clause = " AND ".join(filters)

    rows = conn.execute(
        f"""
        SELECT embeddings.chunk_id, embeddings.vector
        FROM embeddings
        JOIN chunks ON chunks.id = embeddings.chunk_id
        JOIN entities ON entities.id = chunks.entity_id
        WHERE {where_clause}
        """,
        params,
    ).fetchall()

    scores: List[Dict[str, Any]] = []
    for row in rows:
        vector = deserialize_vector(row["vector"])
        score = _dot(query_vector, vector)
        scores.append({"chunk_id": row["chunk_id"], "score": score})

    scores.sort(key=lambda item: item["score"], reverse=True)
    return scores[:limit]


def _dot(query_vector: List[float], vector) -> float:
    total = 0.0
    for a, b in zip(query_vector, vector):
        total += float(a) * float(b)
    return total
