from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

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
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    if limit < 0:
        limit = 0
    if offset < 0:
        offset = 0

    vectors_enabled = bool(config.get("vectors", {}).get("enabled", False))

    if vectors_enabled:
        requested = limit + offset + 1
        results = hybrid_search(
            conn,
            query=query,
            config=config,
            limit=requested,
            source=source,
            allow_sensitive=allow_sensitive,
        )
    else:
        results = search_entities_bm25(
            conn,
            query=query,
            limit=limit + offset + 1,
            offset=0,
            source=source,
            allow_sensitive=allow_sensitive,
            max_chunks_per_entity=config.get("search", {}).get("max_chunks_per_entity", 3),
        )[0]

    sliced = results[offset : offset + limit]
    has_more = len(results) > offset + limit
    next_cursor = str(offset + limit) if has_more else None
    return sliced, next_cursor
