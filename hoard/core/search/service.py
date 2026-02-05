from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from hoard.core.memory.store import memory_search
from hoard.core.search.bm25 import search_entities_bm25
from hoard.core.search.hybrid import hybrid_search


def search_entities(
    conn,
    *,
    query: str,
    config: dict,
    limit: int = 20,
    offset: int = 0,
    source: str | None = None,
    allow_sensitive: bool = True,
    types: Optional[List[str]] = None,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    if limit < 0:
        limit = 0
    if offset < 0:
        offset = 0

    requested = limit + offset + 1
    normalized_types = _normalize_types(types)
    if source:
        if source == "memory":
            normalized_types = [t for t in normalized_types if t == "memory"]
        else:
            normalized_types = [t for t in normalized_types if t != "memory"]

    entity_results: List[Dict[str, Any]] = []
    if "entity" in normalized_types:
        vectors_enabled = bool(config.get("vectors", {}).get("enabled", False))
        if vectors_enabled:
            entity_results = hybrid_search(
                conn,
                query=query,
                config=config,
                limit=requested,
                source=source,
                allow_sensitive=allow_sensitive,
            )
        else:
            entity_results = search_entities_bm25(
                conn,
                query=query,
                limit=requested,
                offset=0,
                source=source,
                allow_sensitive=allow_sensitive,
                max_chunks_per_entity=config.get("search", {}).get("max_chunks_per_entity", 3),
            )[0]

        for entry in entity_results:
            chunk_scores = [chunk.get("score", 0.0) for chunk in entry.get("chunks", [])]
            entry["entity_score"] = max(chunk_scores) if chunk_scores else 0.0
            entry["updated_at"] = entry.get("entity_updated_at")
            entry["result_type"] = "entity"

    memory_results: List[Dict[str, Any]] = []
    if "memory" in normalized_types:
        memory_rows = memory_search(conn, query, limit=requested)
        for entry in memory_rows:
            memory_results.append(
                {
                    "result_type": "memory",
                    "entity_id": entry["id"],
                    "entity_title": entry["key"],
                    "source": "memory",
                    "uri": None,
                    "memory_key": entry["key"],
                    "updated_at": entry["updated_at"],
                    "memory_score": entry.get("score", 0.0),
                    "tags": entry.get("tags", []),
                    "metadata": entry.get("metadata"),
                    "chunks": [
                        {
                            "chunk_id": entry["id"],
                            "content": entry["content"],
                            "score": entry.get("score", 0.0),
                            "char_offset_start": None,
                            "char_offset_end": None,
                            "chunk_type": "memory",
                        }
                    ],
                }
            )

    rrf_k = int(config.get("search", {}).get("rrf_k", 60))
    for idx, entry in enumerate(entity_results):
        entry["score"] = 1.0 / (rrf_k + idx + 1)
    for idx, entry in enumerate(memory_results):
        entry["score"] = 1.0 / (rrf_k + idx + 1)

    results = entity_results + memory_results
    results.sort(key=lambda item: (_score_key(item), _updated_key(item)), reverse=True)

    sliced = results[offset : offset + limit]
    has_more = len(results) > offset + limit
    next_cursor = str(offset + limit) if has_more else None
    return sliced, next_cursor


def _normalize_types(types: Optional[List[str]]) -> List[str]:
    if not types:
        return ["entity", "memory"]
    items: List[str]
    if isinstance(types, str):
        items = [value.strip() for value in types.split(",") if value.strip()]
    else:
        items = list(types)

    normalized: List[str] = []
    for item in items:
        if not item:
            continue
        value = item.strip().lower()
        if value in {"entity", "memory"} and value not in normalized:
            normalized.append(value)
    return normalized


def _updated_key(item: Dict[str, Any]) -> float:
    value = item.get("updated_at")
    if not value:
        return 0.0
    try:
        iso_value = value.replace("Z", "+00:00")
        return datetime.fromisoformat(iso_value).timestamp()
    except ValueError:
        return 0.0


def _score_key(item: Dict[str, Any]) -> float:
    score = item.get("score")
    if score is None:
        return 0.0
    try:
        return float(score)
    except (TypeError, ValueError):
        return 0.0
