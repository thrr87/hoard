from __future__ import annotations

import json
import uuid
from typing import Any, Dict, List, Optional

from hoard.core.orchestrator.utils import dumps, now_iso


class EventError(Exception):
    pass


def publish_event(
    conn,
    *,
    event_type: str,
    payload: Dict[str, Any],
    agent_id: Optional[str] = None,
    task_id: Optional[str] = None,
    workflow_id: Optional[str] = None,
    expires_at: Optional[str] = None,
) -> Dict[str, Any]:
    if not event_type:
        raise EventError("event_type is required")
    event_id = f"evt-{uuid.uuid4()}"
    conn.execute(
        """
        INSERT INTO events
        (id, event_type, agent_id, task_id, workflow_id, payload, published_at, expires_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event_id,
            event_type,
            agent_id,
            task_id,
            workflow_id,
            dumps(payload) or "{}",
            now_iso(),
            expires_at,
        ),
    )
    return {"event_id": event_id}


def poll_events(
    conn,
    *,
    since: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    now = now_iso()
    if since:
        rows = conn.execute(
            """
            SELECT * FROM events
            WHERE published_at > ?
              AND (expires_at IS NULL OR expires_at > ?)
            ORDER BY published_at ASC
            LIMIT ?
            """,
            (since, now, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT * FROM events
            WHERE expires_at IS NULL OR expires_at > ?
            ORDER BY published_at ASC
            LIMIT ?
            """,
            (now, limit),
        ).fetchall()

    results: List[Dict[str, Any]] = []
    for row in rows:
        results.append(_row_to_dict(row))
    return results


def _row_to_dict(row) -> Dict[str, Any]:
    payload = json.loads(row["payload"]) if row["payload"] else {}
    return {
        "event_id": row["id"],
        "event_type": row["event_type"],
        "agent_id": row["agent_id"],
        "task_id": row["task_id"],
        "workflow_id": row["workflow_id"],
        "payload": payload,
        "published_at": row["published_at"],
        "expires_at": row["expires_at"],
    }
