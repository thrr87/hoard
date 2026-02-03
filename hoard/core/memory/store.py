from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from hoard.core.ingest.hash import compute_content_hash


class MemoryError(Exception):
    pass


def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def memory_put(
    conn,
    *,
    key: str,
    content: str,
    tags: Optional[List[str]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if not key:
        raise MemoryError("Memory key is required")
    if content is None:
        raise MemoryError("Memory content is required")

    tags = tags or []
    tags_text = " ".join(tags)
    tags_json = json.dumps(tags) if tags else None
    metadata_json = json.dumps(metadata) if metadata else None

    entry_id = compute_content_hash(f"memory:{key}")
    now = _now_iso()

    conn.execute(
        """
        INSERT INTO memory_entries (
            id, key, content, tags, tags_text, metadata, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            content = excluded.content,
            tags = excluded.tags,
            tags_text = excluded.tags_text,
            metadata = excluded.metadata,
            updated_at = excluded.updated_at
        """,
        (entry_id, key, content, tags_json, tags_text, metadata_json, now, now),
    )
    conn.commit()

    return {
        "id": entry_id,
        "key": key,
        "content": content,
        "tags": tags,
        "metadata": metadata,
        "created_at": now,
        "updated_at": now,
    }


def memory_get(conn, key: str) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        "SELECT * FROM memory_entries WHERE key = ?",
        (key,),
    ).fetchone()
    if not row:
        return None

    return _row_to_entry(row)


def memory_search(conn, query: str, limit: int = 20) -> List[Dict[str, Any]]:
    if not query.strip():
        return []

    rows = conn.execute(
        """
        SELECT memory_entries.*,
               -bm25(memory_fts) AS score
        FROM memory_fts
        JOIN memory_entries ON memory_fts.rowid = memory_entries.rowid
        WHERE memory_fts MATCH ?
        ORDER BY score DESC
        LIMIT ?
        """,
        (query, limit),
    ).fetchall()

    results = []
    for row in rows:
        entry = _row_to_entry(row)
        entry["score"] = row["score"]
        results.append(entry)
    return results


def _row_to_entry(row) -> Dict[str, Any]:
    tags = json.loads(row["tags"]) if row["tags"] else []
    metadata = json.loads(row["metadata"]) if row["metadata"] else None
    return {
        "id": row["id"],
        "key": row["key"],
        "content": row["content"],
        "tags": tags,
        "metadata": metadata,
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }
