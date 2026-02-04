from __future__ import annotations

import json
import uuid
from datetime import datetime

from hoard.core.ingest.hash import compute_content_hash

VERSION = 3


def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def up(conn) -> None:
    try:
        rows = conn.execute(
            "SELECT id, key, content, tags, metadata, created_at, updated_at FROM memory_entries"
        ).fetchall()
    except Exception:
        return

    if not rows:
        return

    for row in rows:
        memory_id = row[0]
        key = row[1]
        content = row[2]
        tags_json = row[3]
        metadata_json = row[4]
        created_at = row[5] or row[6] or _now_iso()

        if not memory_id:
            memory_id = compute_content_hash(f"memory:{key or content}")

        legacy_context = {
            "legacy_key": key,
        }
        if metadata_json:
            try:
                legacy_context["metadata"] = json.loads(metadata_json)
            except json.JSONDecodeError:
                legacy_context["metadata_raw"] = metadata_json

        conn.execute(
            """
            INSERT OR IGNORE INTO memories (
                id, content, memory_type, slot, scope_type, scope_id,
                source_agent, source_agent_version, source_session_id,
                source_conversation_id, source_context, created_at,
                expires_at, superseded_by, superseded_at,
                retracted_at, retracted_by, retraction_reason,
                sensitivity
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                memory_id,
                content,
                "context",
                None,
                "user",
                None,
                "legacy",
                None,
                None,
                None,
                json.dumps(legacy_context),
                created_at,
                None,
                None,
                None,
                None,
                None,
                None,
                "normal",
            ),
        )

        conn.execute(
            "INSERT OR IGNORE INTO memory_counters (memory_id) VALUES (?)",
            (memory_id,),
        )

        tags = []
        if tags_json:
            try:
                tags = json.loads(tags_json)
            except json.JSONDecodeError:
                tags = []
        for tag in tags or []:
            if not tag:
                continue
            tag_value = str(tag).lower()
            conn.execute(
                "INSERT OR IGNORE INTO memory_tags (memory_id, tag) VALUES (?, ?)",
                (memory_id, tag_value),
            )

        event_id = str(uuid.uuid4())
        conn.execute(
            """
            INSERT OR IGNORE INTO memory_events (
                id, memory_id, event_type, event_at, actor, snapshot, event_data
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                memory_id,
                "created",
                created_at,
                "system:migration",
                None,
                json.dumps({"source": "memory_entries"}),
            ),
        )

    conn.commit()


def down(conn) -> None:
    raise NotImplementedError("Rollback not supported for this migration")
