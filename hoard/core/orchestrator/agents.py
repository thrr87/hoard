from __future__ import annotations

import json
import secrets
import uuid
from typing import Any, Dict, Iterable, List, Optional

from hoard.core.errors import HoardError
from hoard.core.orchestrator.utils import dumps, now_iso
from hoard.core.security.agent_tokens import register_agent as register_agent_token
from hoard.core.security.agent_tokens import delete_agent as delete_agent_token


class AgentError(HoardError):
    pass


def _normalize_list(items: Optional[Iterable[str]]) -> List[str]:
    if not items:
        return []
    return sorted({str(item).strip() for item in items if str(item).strip()})


def register_agent(
    conn,
    *,
    config: dict,
    name: str,
    agent_type: str,
    capabilities: Optional[Iterable[str]] = None,
    scopes: Optional[Iterable[str]] = None,
    metadata: Optional[Dict[str, Any]] = None,
    max_concurrent_tasks: int = 1,
    default_model: Optional[str] = None,
    model_provider: Optional[str] = None,
    overwrite: bool = False,
) -> Dict[str, Any]:
    if not name:
        raise AgentError("Agent name is required")
    if not agent_type:
        raise AgentError("agent_type is required")

    scopes_list = _normalize_list(scopes)
    caps_list = _normalize_list(capabilities or scopes_list)

    if not scopes_list:
        scopes_list = _default_scopes(config)

    existing = conn.execute("SELECT id FROM agents WHERE name = ?", (name,)).fetchone()
    if existing and not overwrite:
        raise AgentError(f"Agent '{name}' already exists")

    agent_id = existing["id"] if existing else str(uuid.uuid4())
    token_value = f"hoard_agt_sk_{secrets.token_hex(16)}"

    register_agent_token(
        conn,
        config=config,
        agent_id=agent_id,
        token=token_value,
        scopes=scopes_list,
        capabilities=caps_list,
        overwrite=bool(existing),
    )

    now = now_iso()
    if existing:
        conn.execute(
            """
            UPDATE agents
            SET agent_type = ?, status = 'active', last_heartbeat_at = NULL,
                default_model = ?, model_provider = ?, max_concurrent_tasks = ?,
                scopes = ?, metadata = ?, deregistered_at = NULL
            WHERE id = ?
            """,
            (
                agent_type,
                default_model,
                model_provider,
                int(max_concurrent_tasks or 1),
                dumps(scopes_list),
                dumps(metadata),
                agent_id,
            ),
        )
    else:
        conn.execute(
            """
            INSERT INTO agents
            (id, name, agent_type, registered_at, status, default_model,
             model_provider, max_concurrent_tasks, scopes, metadata)
            VALUES (?, ?, ?, ?, 'active', ?, ?, ?, ?, ?)
            """,
            (
                agent_id,
                name,
                agent_type,
                now,
                default_model,
                model_provider,
                int(max_concurrent_tasks or 1),
                dumps(scopes_list),
                dumps(metadata),
            ),
        )

    _replace_capabilities(conn, agent_id, caps_list)

    return {
        "agent_id": agent_id,
        "name": name,
        "token": token_value,
        "scopes": scopes_list,
        "capabilities": caps_list,
    }


def heartbeat_agent(conn, *, agent_id: str, status: Optional[str] = None) -> bool:
    if not agent_id:
        raise AgentError("agent_id is required")
    now = now_iso()
    cursor = conn.execute(
        """
        UPDATE agents
        SET last_heartbeat_at = ?, status = ?
        WHERE id = ?
        """,
        (now, status or "active", agent_id),
    )
    return cursor.rowcount > 0


def update_agent_capabilities(
    conn,
    *,
    agent_id: str,
    capabilities: Iterable[str],
) -> bool:
    if not agent_id:
        raise AgentError("agent_id is required")
    caps_list = _normalize_list(capabilities)
    _replace_capabilities(conn, agent_id, caps_list)
    return True


def list_agents(conn) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT * FROM agents ORDER BY registered_at DESC
        """
    ).fetchall()
    output: List[Dict[str, Any]] = []
    for row in rows:
        caps = conn.execute(
            "SELECT capability, proficiency FROM agent_capabilities WHERE agent_id = ?",
            (row["id"],),
        ).fetchall()
        output.append(
            {
                "agent_id": row["id"],
                "name": row["name"],
                "agent_type": row["agent_type"],
                "status": row["status"],
                "registered_at": row["registered_at"],
                "last_heartbeat_at": row["last_heartbeat_at"],
                "default_model": row["default_model"],
                "model_provider": row["model_provider"],
                "max_concurrent_tasks": row["max_concurrent_tasks"],
                "current_task_count": row["current_task_count"],
                "scopes": json.loads(row["scopes"]) if row["scopes"] else [],
                "metadata": json.loads(row["metadata"]) if row["metadata"] else None,
                "capabilities": [dict(cap) for cap in caps],
            }
        )
    return output


def deregister_agent(conn, *, agent_id: str) -> bool:
    if not agent_id:
        raise AgentError("agent_id is required")
    now = now_iso()
    cursor = conn.execute(
        """
        UPDATE agents
        SET status = 'deregistered', deregistered_at = ?
        WHERE id = ?
        """,
        (now, agent_id),
    )
    delete_agent_token(conn, agent_id)
    return cursor.rowcount > 0


def _replace_capabilities(conn, agent_id: str, capabilities: List[str]) -> None:
    conn.execute("DELETE FROM agent_capabilities WHERE agent_id = ?", (agent_id,))
    for capability in capabilities:
        conn.execute(
            """
            INSERT INTO agent_capabilities (agent_id, capability)
            VALUES (?, ?)
            """,
            (agent_id, capability),
        )


def _default_scopes(config: dict) -> List[str]:
    defaults = config.get("orchestrator", {}).get("default_scopes")
    if defaults:
        return _normalize_list(defaults)
    return [
        "data.search",
        "data.get",
        "memory.read",
        "task.claim",
        "task.execute",
        "artifact.read",
        "artifact.write",
        "event.read",
        "cost.write",
    ]
