from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from hoard.core.embeddings.store import deserialize_vector
from hoard.core.search.ann.hnsw import HnswAnnBackend


def vector_search(
    conn,
    *,
    query_vector: List[float],
    model_name: str,
    limit: int = 20,
    candidate_chunk_ids: Optional[Iterable[str]] = None,
    source: str | None = None,
    allow_sensitive: bool = True,
    max_candidates: int = 5000,
    ann_enabled: bool = False,
    ann_config: Optional[dict] = None,
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
    limit_clause = ""
    if candidate_chunk_ids is None:
        candidate_cap = max(1, int(max_candidates))
        limit_clause = f" LIMIT {candidate_cap}"

    rows = conn.execute(
        f"""
        SELECT embeddings.chunk_id, embeddings.vector
        FROM embeddings
        JOIN chunks ON chunks.id = embeddings.chunk_id
        JOIN entities ON entities.id = chunks.entity_id
        WHERE {where_clause}
        {limit_clause}
        """,
        params,
    ).fetchall()

    vectors: List[tuple[str, List[float]]] = []
    scores: List[Dict[str, Any]] = []
    for row in rows:
        vector = deserialize_vector(row["vector"])
        vec = [float(v) for v in vector]
        vectors.append((row["chunk_id"], vec))
        score = _dot(query_vector, vec)
        scores.append({"chunk_id": row["chunk_id"], "score": score})

    if ann_enabled and vectors:
        cfg = ann_config or {}
        try:
            backend = HnswAnnBackend()
            ann_results = backend.search(
                query_vector=query_vector,
                candidates=vectors,
                limit=limit,
                ef_search=int(cfg.get("ef_search", 64) or 64),
                m=int(cfg.get("m", 16) or 16),
                ef_construction=int(cfg.get("ef_construction", 200) or 200),
            )
            return [
                {"chunk_id": result.item_id, "score": result.score}
                for result in ann_results
            ]
        except Exception:
            # Graceful fallback to exact scan when ANN backend is unavailable.
            pass

    scores.sort(key=lambda item: item["score"], reverse=True)
    return scores[:limit]


def _dot(query_vector: List[float], vector) -> float:
    total = 0.0
    for a, b in zip(query_vector, vector):
        total += float(a) * float(b)
    return total
