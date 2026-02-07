from __future__ import annotations

import secrets
from typing import Any, Dict, List

from hoard.core.mcp.scopes import require_any_scope
from hoard.core.security.agent_tokens import delete_agent, list_agents, register_agent


TOOL_DEFINITIONS: List[Dict[str, Any]] = [
    {
        "name": "agent_register",
        "description": "Register an agent token (admin).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "agent_id": {"type": "string"},
                "scopes": {"type": "array", "items": {"type": "string"}},
                "overwrite": {"type": "boolean"},
                "token": {"type": "string"},
            },
            "required": ["agent_id"],
        },
    },
    {
        "name": "agent_list",
        "description": "List registered agent tokens (admin).",
        "inputSchema": {"type": "object", "properties": {"token": {"type": "string"}}, "required": []},
    },
    {
        "name": "agent_remove",
        "description": "Remove registered agent token (admin).",
        "inputSchema": {
            "type": "object",
            "properties": {"agent_id": {"type": "string"}, "token": {"type": "string"}},
            "required": ["agent_id"],
        },
    },
]


WRITE_TOOLS = {"agent_register", "agent_remove"}


def dispatch_tool(tool: str, arguments: Dict[str, Any], conn, config: Dict[str, Any], token):
    if tool == "agent_register":
        _require_admin(token)
        agent_id = arguments.get("agent_id")
        scopes = arguments.get("scopes") or []
        overwrite = bool(arguments.get("overwrite", False))
        if not agent_id:
            raise ValueError("agent_id is required")
        token_value = arguments.get("token") or f"hoard_sk_{secrets.token_hex(16)}"
        register_agent(
            conn,
            config=config,
            agent_id=agent_id,
            token=token_value,
            scopes=scopes,
            capabilities=scopes,
            rate_limit_per_hour=int(
                config.get("write", {})
                .get("limits", {})
                .get("per_agent", {})
                .get("max_writes_per_hour", 100)
            ),
            overwrite=overwrite,
        )
        return {"agent_id": agent_id, "token": token_value, "scopes": scopes}

    if tool == "agent_list":
        _require_admin(token)
        agents = list_agents(conn)
        return {
            "agents": [
                {
                    "agent_id": agent.agent_id,
                    "scopes": sorted(agent.scopes),
                    "capabilities": sorted(agent.capabilities),
                    "trust_level": agent.trust_level,
                    "can_access_sensitive": agent.can_access_sensitive,
                    "can_access_restricted": agent.can_access_restricted,
                }
                for agent in agents
            ]
        }

    if tool == "agent_remove":
        _require_admin(token)
        agent_id = arguments.get("agent_id")
        if not agent_id:
            raise ValueError("agent_id is required")
        success = delete_agent(conn, agent_id)
        return {"success": success}

    return None


def _require_admin(token) -> None:
    require_any_scope(token, {"admin"}, message="Missing scopes: admin")
