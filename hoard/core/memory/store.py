from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from hoard.core.ingest.hash import compute_content_hash
from hoard.core.memory.v2.store import MemoryError as V2MemoryError
from hoard.core.memory.v2.store import memory_query as v2_memory_query
from hoard.core.memory.v2.store import memory_write as v2_memory_write
from hoard.core.security.auth import TokenInfo


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
    ttl_days: Optional[int] = None,
    expires_at: Optional[str] = None,
    default_ttl_days: Optional[int] = None,
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
    expires_value = _resolve_expires_at(
        ttl_days=ttl_days,
        expires_at=expires_at,
        default_ttl_days=default_ttl_days,
    )

    source_context = json.dumps({"legacy_key": key, "metadata": metadata} if metadata else {"legacy_key": key})
    try:
        v2_memory_write(
            conn,
            memory_id=entry_id,
            content=content,
            memory_type="context",
            scope_type="user",
            scope_id=None,
            source_agent="legacy",
            source_context=source_context,
            tags=tags,
            expires_at=expires_value,
            config={},
        )
    except V2MemoryError:
        pass

    conn.execute(
        """
        INSERT INTO memory_entries (
            id, key, content, tags, tags_text, metadata, created_at, updated_at, expires_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            content = excluded.content,
            tags = excluded.tags,
            tags_text = excluded.tags_text,
            metadata = excluded.metadata,
            updated_at = excluded.updated_at,
            expires_at = excluded.expires_at
        """,
        (entry_id, key, content, tags_json, tags_text, metadata_json, now, now, expires_value),
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
        "expires_at": expires_value,
    }


def memory_get(conn, key: str) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT * FROM memory_entries
        WHERE key = ?
          AND (expires_at IS NULL OR expires_at > ?)
        """,
        (key, _now_iso()),
    ).fetchone()
    if row:
        return _row_to_entry(row)

    legacy_key_match = f'\"legacy_key\": \"{key}\"'
    memory_row = conn.execute(
        """
        SELECT * FROM memories
        WHERE source_context LIKE ?
          AND (expires_at IS NULL OR expires_at > ?)
          AND retracted_at IS NULL
          AND superseded_at IS NULL
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (f"%{legacy_key_match}%", _now_iso()),
    ).fetchone()
    if not memory_row:
        return None

    metadata = None
    try:
        ctx = json.loads(memory_row["source_context"]) if memory_row["source_context"] else {}
        metadata = ctx.get("metadata")
    except json.JSONDecodeError:
        metadata = None

    return {
        "id": memory_row["id"],
        "key": key,
        "content": memory_row["content"],
        "tags": [],
        "metadata": metadata,
        "created_at": memory_row["created_at"],
        "updated_at": memory_row["created_at"],
    }

def memory_get_by_id(conn, entry_id: str) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT * FROM memory_entries
        WHERE id = ?
          AND (expires_at IS NULL OR expires_at > ?)
        """,
        (entry_id, _now_iso()),
    ).fetchone()
    if not row:
        return None
    return _row_to_entry(row)


def memory_search(
    conn,
    query: str,
    limit: int = 20,
    *,
    agent: Optional[TokenInfo] = None,
    config: Optional[dict] = None,
) -> List[Dict[str, Any]]:
    if not query.strip():
        return []
    results = v2_memory_query(
        conn,
        params={"query": query, "limit": limit},
        agent=agent or _legacy_agent(),
        config=config or {},
    ).get("results", [])

    output = []
    for entry in results:
        key = None
        metadata = None
        if entry.get("source_context"):
            try:
                ctx = json.loads(entry["source_context"])
                key = ctx.get("legacy_key")
                metadata = ctx.get("metadata")
            except json.JSONDecodeError:
                key = None
        output.append(
            {
                "id": entry.get("id"),
                "key": key or entry.get("id"),
                "content": entry.get("content"),
                "tags": entry.get("tags", []),
                "metadata": metadata,
                "created_at": entry.get("created_at"),
                "updated_at": entry.get("created_at"),
                "score": entry.get("score"),
            }
        )
    return output


def _legacy_agent() -> TokenInfo:
    return TokenInfo(
        name="legacy",
        token=None,
        scopes={"memory"},
        capabilities={"memory"},
        trust_level=0.5,
        can_access_sensitive=True,
        can_access_restricted=True,
        requires_user_confirm=False,
        proposal_ttl_days=None,
        rate_limit_per_hour=0,
    )


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
        "expires_at": row["expires_at"],
    }


def memory_prune(conn) -> int:
    now = _now_iso()
    cursor = conn.execute(
        "DELETE FROM memory_entries WHERE expires_at IS NOT NULL AND expires_at <= ?",
        (now,),
    )
    conn.commit()
    return cursor.rowcount


def _resolve_expires_at(
    *,
    ttl_days: Optional[int],
    expires_at: Optional[str],
    default_ttl_days: Optional[int],
) -> Optional[str]:
    if expires_at:
        return expires_at

    if ttl_days is None:
        ttl_days = default_ttl_days

    if ttl_days is None:
        return None

    try:
        ttl_days_int = int(ttl_days)
    except (TypeError, ValueError) as exc:
        raise MemoryError("Invalid ttl_days value") from exc

    if ttl_days_int <= 0:
        return _now_iso()

    expires_dt = datetime.utcnow() + timedelta(days=ttl_days_int)
    return expires_dt.isoformat(timespec="seconds")
