from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

from hoard.core.memory.predicates import active_memory_conditions
from hoard.core.security.auth import TokenInfo


class MemoryError(Exception):
    pass


def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def _parse_json(value: str | None) -> Any:
    if not value:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


def _normalize_tags(tags: Optional[Iterable[str]]) -> list[str]:
    if not tags:
        return []
    return sorted({str(tag).strip().lower() for tag in tags if str(tag).strip()})


def _validate_slot(slot: Optional[str], config: dict) -> None:
    if not slot:
        return
    pattern = config.get("write", {}).get(
        "slots", {}
    ).get("pattern", r"^(pref|fact|ctx|decision|event):[a-z0-9_]+(\.[a-z0-9_]+){0,3}$")
    if not re.match(pattern, slot):
        on_invalid = config.get("write", {}).get("slots", {}).get("on_invalid", "reject")
        if on_invalid == "reject":
            raise MemoryError(f"Invalid slot: {slot}")


def _build_sensitivity_conditions(agent: TokenInfo, table_alias: str = "m") -> list[str]:
    if agent.can_access_restricted:
        return []
    if agent.can_access_sensitive:
        return [f"{table_alias}.sensitivity != 'restricted'"]
    return [f"{table_alias}.sensitivity = 'normal'"]


def _memory_row_to_dict(row) -> Dict[str, Any]:
    return {
        "id": row["id"],
        "content": row["content"],
        "memory_type": row["memory_type"],
        "slot": row["slot"],
        "scope_type": row["scope_type"],
        "scope_id": row["scope_id"],
        "source_agent": row["source_agent"],
        "source_agent_version": row["source_agent_version"],
        "source_session_id": row["source_session_id"],
        "source_conversation_id": row["source_conversation_id"],
        "source_context": row["source_context"],
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
        "superseded_by": row["superseded_by"],
        "superseded_at": row["superseded_at"],
        "retracted_at": row["retracted_at"],
        "retracted_by": row["retracted_by"],
        "retraction_reason": row["retraction_reason"],
        "sensitivity": row["sensitivity"],
    }


def _insert_event(conn, *, memory_id: str, event_type: str, actor: str, event_data: dict | None = None) -> None:
    conn.execute(
        """
        INSERT INTO memory_events (id, memory_id, event_type, event_at, actor, event_data)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid.uuid4()),
            memory_id,
            event_type,
            _now_iso(),
            actor,
            json.dumps(event_data) if event_data else None,
        ),
    )


def _enqueue_job(conn, *, job_type: str, memory_id: str, priority: int = 0, max_retries: int = 3) -> None:
    conn.execute(
        """
        INSERT INTO background_jobs
        (id, job_type, memory_id, status, priority, created_at, max_retries)
        VALUES (?, ?, ?, 'pending', ?, ?, ?)
        """,
        (str(uuid.uuid4()), job_type, memory_id, priority, _now_iso(), max_retries),
    )


def _check_and_update_rate_limit(conn, agent: TokenInfo, config: dict) -> None:
    limit = int(agent.rate_limit_per_hour or 0)
    if limit <= 0:
        return
    window_start = datetime.utcnow().replace(minute=0, second=0, microsecond=0).isoformat()
    row = conn.execute(
        """
        SELECT write_count FROM agent_rate_limits
        WHERE agent_id = ? AND window_start = ?
        """,
        (agent.name, window_start),
    ).fetchone()
    if row and row[0] >= limit:
        raise MemoryError("Write rate limit exceeded")
    if row:
        conn.execute(
            """
            UPDATE agent_rate_limits SET write_count = write_count + 1
            WHERE agent_id = ? AND window_start = ?
            """,
            (agent.name, window_start),
        )
    else:
        conn.execute(
            """
            INSERT INTO agent_rate_limits (agent_id, window_start, write_count)
            VALUES (?, ?, 1)
            """,
            (agent.name, window_start),
        )


def memory_write(
    conn,
    *,
    memory_id: Optional[str] = None,
    content: str,
    memory_type: str,
    scope_type: str,
    scope_id: Optional[str],
    source_agent: str,
    source_agent_version: Optional[str] = None,
    source_session_id: Optional[str] = None,
    source_conversation_id: Optional[str] = None,
    source_context: Optional[str] = None,
    slot: Optional[str] = None,
    tags: Optional[Iterable[str]] = None,
    relations: Optional[Iterable[dict]] = None,
    expires_at: Optional[str] = None,
    sensitivity: str = "normal",
    actor: Optional[str] = None,
    agent: Optional[TokenInfo] = None,
    config: Optional[dict] = None,
) -> Dict[str, Any]:
    if not content:
        raise MemoryError("Memory content is required")
    if not memory_type:
        raise MemoryError("memory_type is required")
    if not scope_type:
        raise MemoryError("scope_type is required")
    if not source_agent:
        raise MemoryError("source_agent is required")

    config = config or {}
    _validate_slot(slot, config)

    if scope_type == "user":
        scope_id = None
    elif not scope_id:
        raise MemoryError("scope_id required for non-user scope")

    tags_norm = _normalize_tags(tags)

    memory_id = memory_id or str(uuid.uuid4())
    now = _now_iso()

    if agent:
        _check_and_update_rate_limit(conn, agent, config)

    conn.execute(
        """
        INSERT INTO memories (
            id, content, memory_type, slot, scope_type, scope_id,
            source_agent, source_agent_version, source_session_id,
            source_conversation_id, source_context, created_at,
            expires_at, sensitivity
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            memory_id,
            content,
            memory_type,
            slot,
            scope_type,
            scope_id,
            source_agent,
            source_agent_version,
            source_session_id,
            source_conversation_id,
            source_context,
            now,
            expires_at,
            sensitivity,
        ),
    )

    conn.execute(
        "INSERT INTO memory_counters (memory_id) VALUES (?)",
        (memory_id,),
    )

    for tag in tags_norm:
        conn.execute(
            "INSERT INTO memory_tags (memory_id, tag) VALUES (?, ?)",
            (memory_id, tag),
        )

    for relation in relations or []:
        related_uri = relation.get("related_uri") if isinstance(relation, dict) else None
        relation_type = relation.get("relation_type") if isinstance(relation, dict) else None
        if related_uri:
            conn.execute(
                """
                INSERT INTO memory_relations (memory_id, related_uri, relation_type)
                VALUES (?, ?, ?)
                """,
                (memory_id, related_uri, relation_type or "related"),
            )

    _insert_event(conn, memory_id=memory_id, event_type="created", actor=actor or source_agent)

    _enqueue_job(conn, job_type="embed_memory", memory_id=memory_id, priority=0)
    _enqueue_job(conn, job_type="detect_duplicates", memory_id=memory_id, priority=0)
    _enqueue_job(conn, job_type="detect_conflicts", memory_id=memory_id, priority=0)

    return {
        "id": memory_id,
        "content": content,
        "memory_type": memory_type,
        "slot": slot,
        "scope_type": scope_type,
        "scope_id": scope_id,
        "source_agent": source_agent,
        "source_agent_version": source_agent_version,
        "source_session_id": source_session_id,
        "source_conversation_id": source_conversation_id,
        "source_context": source_context,
        "created_at": now,
        "expires_at": expires_at,
        "sensitivity": sensitivity,
        "tags": tags_norm,
        "relations": relations or [],
    }


def memory_get(conn, memory_id: str) -> Optional[Dict[str, Any]]:
    row = conn.execute("SELECT * FROM memories WHERE id = ?", (memory_id,)).fetchone()
    if not row:
        return None
    entry = _memory_row_to_dict(row)
    tags = conn.execute(
        "SELECT tag FROM memory_tags WHERE memory_id = ? ORDER BY tag",
        (memory_id,),
    ).fetchall()
    relations = conn.execute(
        "SELECT related_uri, relation_type FROM memory_relations WHERE memory_id = ?",
        (memory_id,),
    ).fetchall()
    entry["tags"] = [r[0] for r in tags]
    entry["relations"] = [dict(r) for r in relations]
    return entry


def memory_retract(
    conn,
    *,
    memory_id: str,
    actor: str,
    reason: Optional[str] = None,
) -> bool:
    now = _now_iso()
    cursor = conn.execute(
        """
        UPDATE memories
        SET retracted_at = ?, retracted_by = ?, retraction_reason = ?
        WHERE id = ?
        """,
        (now, actor, reason, memory_id),
    )
    if cursor.rowcount:
        _insert_event(
            conn,
            memory_id=memory_id,
            event_type="retracted",
            actor=actor,
            event_data={"reason": reason} if reason else None,
        )
    return cursor.rowcount > 0


def memory_supersede(
    conn,
    *,
    memory_id: str,
    superseded_by: str,
    actor: str,
) -> bool:
    now = _now_iso()
    cursor = conn.execute(
        """
        UPDATE memories
        SET superseded_by = ?, superseded_at = ?
        WHERE id = ?
        """,
        (superseded_by, now, memory_id),
    )
    if cursor.rowcount:
        _insert_event(
            conn,
            memory_id=memory_id,
            event_type="superseded",
            actor=actor,
            event_data={"superseded_by": superseded_by},
        )
    return cursor.rowcount > 0


def memory_propose(
    conn,
    *,
    proposed_memory: Dict[str, Any],
    proposed_by: str,
    config: dict,
    ttl_days: Optional[int] = None,
) -> Dict[str, Any]:
    now = datetime.utcnow()
    max_days = int(config.get("write", {}).get("proposals", {}).get("max_ttl_days", 30))
    default_days = int(
        config.get("write", {}).get("proposals", {}).get("default_ttl_days", 7)
    )
    ttl = ttl_days or default_days
    ttl = min(ttl, max_days)
    expires_at = (now + timedelta(days=ttl)).isoformat(timespec="seconds")

    proposal_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO memory_proposals
        (id, proposed_memory, proposed_by, proposed_at, expires_at, status)
        VALUES (?, ?, ?, ?, ?, 'pending')
        """,
        (
            proposal_id,
            json.dumps(proposed_memory),
            proposed_by,
            now.isoformat(timespec="seconds"),
            expires_at,
        ),
    )
    return {
        "id": proposal_id,
        "proposed_by": proposed_by,
        "proposed_at": now.isoformat(timespec="seconds"),
        "expires_at": expires_at,
        "status": "pending",
    }


def memory_review(
    conn,
    *,
    proposal_id: str,
    approved: bool,
    reviewer: str,
    config: dict,
) -> Dict[str, Any]:
    row = conn.execute(
        "SELECT * FROM memory_proposals WHERE id = ?",
        (proposal_id,),
    ).fetchone()
    if not row:
        raise MemoryError("Proposal not found")

    now = _now_iso()
    if approved:
        proposed = _parse_json(row["proposed_memory"]) or {}
        memory = memory_write(
            conn,
            content=proposed.get("content"),
            memory_type=proposed.get("memory_type", "context"),
            scope_type=proposed.get("scope_type", "user"),
            scope_id=proposed.get("scope_id"),
            source_agent=proposed.get("source_agent", reviewer),
            source_agent_version=proposed.get("source_agent_version"),
            source_session_id=proposed.get("source_session_id"),
            source_conversation_id=proposed.get("source_conversation_id"),
            source_context=proposed.get("source_context"),
            slot=proposed.get("slot"),
            tags=proposed.get("tags"),
            relations=proposed.get("relations"),
            expires_at=proposed.get("expires_at"),
            sensitivity=proposed.get("sensitivity", "normal"),
            actor=reviewer,
            config=config,
        )
        conn.execute(
            """
            UPDATE memory_proposals
            SET status = 'approved', reviewed_at = ?, reviewed_by = ?
            WHERE id = ?
            """,
            (now, reviewer, proposal_id),
        )
        return {"status": "approved", "memory": memory}

    conn.execute(
        """
        UPDATE memory_proposals
        SET status = 'rejected', reviewed_at = ?, reviewed_by = ?
        WHERE id = ?
        """,
        (now, reviewer, proposal_id),
    )
    return {"status": "rejected"}


def conflicts_list(conn, *, status: Optional[str] = None) -> List[Dict[str, Any]]:
    if status == "unresolved":
        rows = conn.execute(
            "SELECT * FROM memory_conflicts WHERE resolved_at IS NULL ORDER BY detected_at DESC"
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM memory_conflicts ORDER BY detected_at DESC").fetchall()
    return [dict(row) for row in rows]


def conflict_resolve(conn, *, conflict_id: str, resolution: str, resolved_by: str) -> bool:
    now = _now_iso()
    cursor = conn.execute(
        """
        UPDATE memory_conflicts
        SET resolved_at = ?, resolution = ?, resolved_by = ?
        WHERE id = ?
        """,
        (now, resolution, resolved_by, conflict_id),
    )
    return cursor.rowcount > 0


def duplicates_list(conn, *, status: Optional[str] = None) -> List[Dict[str, Any]]:
    if status == "unresolved":
        rows = conn.execute(
            "SELECT * FROM memory_duplicates WHERE resolved_at IS NULL ORDER BY detected_at DESC"
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM memory_duplicates ORDER BY detected_at DESC").fetchall()
    return [dict(row) for row in rows]


def duplicate_resolve(conn, *, duplicate_id: str, resolution: str) -> bool:
    now = _now_iso()
    cursor = conn.execute(
        """
        UPDATE memory_duplicates
        SET resolved_at = ?, resolution = ?
        WHERE id = ?
        """,
        (now, resolution, duplicate_id),
    )
    return cursor.rowcount > 0


def memory_query(
    conn,
    *,
    params: Dict[str, Any],
    agent: TokenInfo,
    config: dict,
) -> Dict[str, Any]:
    query = (params.get("query") or "").strip()
    limit = int(params.get("limit", 20))
    slot = params.get("slot")
    scope_type = params.get("scope_type")
    scope_id = params.get("scope_id")
    memory_type = params.get("memory_type")
    tags = _normalize_tags(params.get("tags"))

    now = _now_iso()
    conditions, base_params = active_memory_conditions(now)
    conditions.extend(_build_sensitivity_conditions(agent))

    if scope_type:
        conditions.append("m.scope_type = ?")
        base_params.append(scope_type)
    if scope_id is not None:
        conditions.append("m.scope_id = ?")
        base_params.append(scope_id)
    if memory_type:
        if isinstance(memory_type, list):
            placeholders = ",".join("?" for _ in memory_type)
            conditions.append(f"m.memory_type IN ({placeholders})")
            base_params.extend(memory_type)
        else:
            conditions.append("m.memory_type = ?")
            base_params.append(memory_type)

    tag_filter = ""
    tag_params: list[str] = []
    if tags:
        tag_filter = " AND " + " AND ".join(
            [
                f"EXISTS (SELECT 1 FROM memory_tags t{i} WHERE t{i}.memory_id = m.id AND t{i}.tag = ?)"
                for i in range(len(tags))
            ]
        )
        tag_params.extend(tags)

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    results: list[dict] = []
    score_map: dict[str, float] = {}

    union_multiplier = float(config.get("write", {}).get("query", {}).get("union_multiplier", 2))
    union_limit = int(limit * union_multiplier)

    if query:
        fts_rows = conn.execute(
            f"""
            SELECT m.id, bm25(memories_fts) AS bm25_score
            FROM memories_fts
            JOIN memories m ON memories_fts.rowid = m.rowid
            WHERE memories_fts MATCH ? AND {where_clause}{tag_filter}
            ORDER BY bm25(memories_fts)
            LIMIT ?
            """,
            [query, *base_params, *tag_params, union_limit * 5],
        ).fetchall()

        fts_scores: dict[str, float] = {}
        if fts_rows:
            fts_scores = {row["id"]: 1.0 / (1.0 + row["bm25_score"]) for row in fts_rows}
            max_fts = max(fts_scores.values()) if fts_scores else 1.0
            for mem_id, score in fts_scores.items():
                fts_scores[mem_id] = score / max_fts

        vec_scores: dict[str, float] = {}
        vectors_enabled = bool(
            config.get("write", {}).get("embeddings", {}).get("enabled", False)
            or config.get("vectors", {}).get("enabled", False)
        )
        if vectors_enabled:
            try:
                from sentence_transformers import SentenceTransformer
                import numpy as np

                model_cfg = config.get("write", {}).get("embeddings", {}).get("active_model", {})
                model_name = model_cfg.get("name", "sentence-transformers/all-MiniLM-L6-v2")
                model = SentenceTransformer(model_name)
                query_vec = model.encode([query], normalize_embeddings=True)[0]

                emb_rows = conn.execute(
                    f"""
                    SELECT e.memory_id, e.embedding, e.dimensions
                    FROM memory_embeddings e
                    JOIN memories m ON m.id = e.memory_id
                    WHERE {where_clause}{tag_filter}
                    """,
                    [*base_params, *tag_params],
                ).fetchall()

                vec_scores = {}
                for row in emb_rows:
                    vec = np.frombuffer(row["embedding"], dtype="<f4")
                    if vec.shape[0] != row["dimensions"]:
                        continue
                    sim = float(np.dot(query_vec, vec))
                    vec_scores[row["memory_id"]] = sim

                if vec_scores:
                    max_vec = max(vec_scores.values())
                    min_vec = min(vec_scores.values())
                    denom = (max_vec - min_vec) or 1.0
                    for mem_id, score in vec_scores.items():
                        vec_scores[mem_id] = (score - min_vec) / denom
            except Exception:
                vec_scores = {}

        vector_weight = float(config.get("write", {}).get("query", {}).get("hybrid_weight_vector", 0.6))
        fts_weight = float(config.get("write", {}).get("query", {}).get("hybrid_weight_fts", 0.4))
        for mem_id in set(fts_scores.keys()) | set(vec_scores.keys()):
            score_map[mem_id] = (fts_scores.get(mem_id, 0.0) * fts_weight) + (
                vec_scores.get(mem_id, 0.0) * vector_weight
            )

    slot_bonus = float(config.get("write", {}).get("query", {}).get("slot_match_bonus", 0.1))
    slot_baseline = float(config.get("write", {}).get("query", {}).get("slot_only_baseline", 0.5))

    if slot:
        slot_rows = conn.execute(
            f"""
            SELECT m.id
            FROM memories m
            WHERE {where_clause} AND m.slot = ?{tag_filter}
            ORDER BY m.created_at DESC
            LIMIT ?
            """,
            [*base_params, slot, *tag_params, union_limit],
        ).fetchall()
        for row in slot_rows:
            mem_id = row["id"]
            if mem_id in score_map:
                score_map[mem_id] += slot_bonus
            else:
                score_map[mem_id] = slot_baseline

    if not query and not slot:
        rows = conn.execute(
            f"""
            SELECT m.id, m.created_at
            FROM memories m
            WHERE {where_clause}{tag_filter}
            ORDER BY m.created_at DESC
            LIMIT ?
            """,
            [*base_params, *tag_params, limit],
        ).fetchall()
        ids = [row["id"] for row in rows]
        return {"results": _fetch_memories(conn, ids)}

    sorted_ids = [
        mem_id for mem_id, _ in sorted(score_map.items(), key=lambda item: item[1], reverse=True)
    ][:limit]

    memories = _fetch_memories(conn, sorted_ids)
    score_lookup = score_map
    for entry in memories:
        entry["score"] = score_lookup.get(entry["id"], 0.0)
        results.append(entry)

    return {"results": results}


def _fetch_memories(conn, ids: List[str]) -> List[Dict[str, Any]]:
    if not ids:
        return []
    placeholders = ",".join("?" for _ in ids)
    rows = conn.execute(
        f"SELECT * FROM memories WHERE id IN ({placeholders})",
        ids,
    ).fetchall()
    row_map = {row["id"]: row for row in rows}

    results: list[Dict[str, Any]] = []
    for memory_id in ids:
        row = row_map.get(memory_id)
        if not row:
            continue
        entry = _memory_row_to_dict(row)
        tags = conn.execute(
            "SELECT tag FROM memory_tags WHERE memory_id = ? ORDER BY tag",
            (memory_id,),
        ).fetchall()
        entry["tags"] = [r[0] for r in tags]
        results.append(entry)
    return results
