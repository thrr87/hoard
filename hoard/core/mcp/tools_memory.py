from __future__ import annotations

from typing import Any, Dict, List

from hoard.core.memory.store import memory_get, memory_put, memory_search
from hoard.core.memory.v2.store import (
    conflict_resolve,
    conflicts_list,
    duplicate_resolve,
    duplicates_list,
    memory_get as memory_get_v2,
    memory_propose,
    memory_query as memory_query_v2,
    memory_retract,
    memory_review,
    memory_supersede,
    memory_write,
)
from hoard.core.security.errors import ScopeError


TOOL_DEFINITIONS: List[Dict[str, Any]] = [
    {
        "name": "memory_get",
        "description": "Fetch a memory entry by key or id.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Memory id.",
                },
                "key": {
                    "type": "string",
                    "description": "Memory key.",
                },
                "token": {
                    "type": "string",
                    "description": "Auth token (if HOARD_TOKEN env not set).",
                },
            },
            "required": [],
        },
    },
    {
        "name": "memory_put",
        "description": "Store a memory entry.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "key": {
                    "type": "string",
                    "description": "Memory key.",
                },
                "content": {
                    "type": "string",
                    "description": "Memory content.",
                },
                "tags": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional tags.",
                },
                "metadata": {
                    "type": "object",
                    "description": "Optional metadata object.",
                },
                "ttl_days": {
                    "type": "integer",
                    "description": "Optional time-to-live in days.",
                },
                "expires_at": {
                    "type": "string",
                    "description": "Optional ISO timestamp when the memory expires.",
                },
                "token": {
                    "type": "string",
                    "description": "Auth token (if HOARD_TOKEN env not set).",
                },
            },
            "required": ["key", "content"],
        },
    },
    {
        "name": "memory_write",
        "description": "Create a structured memory record.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "content": {"type": "string"},
                "memory_type": {"type": "string"},
                "slot": {"type": "string"},
                "scope_type": {"type": "string"},
                "scope_id": {"type": "string"},
                "source_agent": {"type": "string"},
                "source_agent_version": {"type": "string"},
                "source_session_id": {"type": "string"},
                "source_conversation_id": {"type": "string"},
                "source_context": {"type": "string"},
                "tags": {"type": "array", "items": {"type": "string"}},
                "relations": {"type": "array", "items": {"type": "object"}},
                "expires_at": {"type": "string"},
                "sensitivity": {"type": "string"},
                "token": {"type": "string"},
            },
            "required": ["content"],
        },
    },
    {
        "name": "memory_query",
        "description": "Query structured memories (hybrid/slot/list).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "limit": {"type": "integer", "default": 20, "minimum": 1, "maximum": 100},
                "slot": {"type": "string"},
                "scope_type": {"type": "string"},
                "scope_id": {"type": "string"},
                "memory_type": {"type": "string"},
                "tags": {"type": "array", "items": {"type": "string"}},
                "token": {"type": "string"},
            },
            "required": [],
        },
    },
    {
        "name": "memory_retract",
        "description": "Retract a memory.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "reason": {"type": "string"},
                "token": {"type": "string"},
            },
            "required": ["id"],
        },
    },
    {
        "name": "memory_supersede",
        "description": "Supersede a memory.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "superseded_by": {"type": "string"},
                "token": {"type": "string"},
            },
            "required": ["id", "superseded_by"],
        },
    },
    {
        "name": "memory_propose",
        "description": "Propose a memory for review.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "memory": {"type": "object"},
                "ttl_days": {"type": "integer"},
                "token": {"type": "string"},
            },
            "required": ["memory"],
        },
    },
    {
        "name": "memory_review",
        "description": "Approve or reject a memory proposal.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "proposal_id": {"type": "string"},
                "approved": {"type": "boolean"},
                "token": {"type": "string"},
            },
            "required": ["proposal_id", "approved"],
        },
    },
    {
        "name": "memory_search",
        "description": "Search memory entries.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search query string.",
                },
                "limit": {
                    "type": "integer",
                    "default": 20,
                    "minimum": 1,
                    "maximum": 100,
                    "description": "Maximum number of entries to return.",
                },
                "token": {
                    "type": "string",
                    "description": "Auth token (if HOARD_TOKEN env not set).",
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "conflicts_list",
        "description": "List memory conflicts.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "status": {"type": "string", "description": "Optional status filter."},
                "token": {"type": "string"},
            },
            "required": [],
        },
    },
    {
        "name": "conflict_resolve",
        "description": "Resolve a memory conflict.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "conflict_id": {"type": "string"},
                "resolution": {"type": "string"},
                "token": {"type": "string"},
            },
            "required": ["conflict_id", "resolution"],
        },
    },
    {
        "name": "duplicates_list",
        "description": "List memory duplicates.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "status": {"type": "string"},
                "token": {"type": "string"},
            },
            "required": [],
        },
    },
    {
        "name": "duplicate_resolve",
        "description": "Resolve a duplicate cluster.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "duplicate_id": {"type": "string"},
                "resolution": {"type": "string"},
                "token": {"type": "string"},
            },
            "required": ["duplicate_id", "resolution"],
        },
    },
]


WRITE_TOOLS = {
    "memory_put",
    "memory_write",
    "memory_retract",
    "memory_supersede",
    "memory_propose",
    "memory_review",
    "conflict_resolve",
    "duplicate_resolve",
}


def dispatch_tool(tool: str, arguments: Dict[str, Any], conn, config: Dict[str, Any], token):
    if tool == "memory_get":
        _require_read(token)
        memory_id = arguments.get("id")
        if memory_id:
            return {"memory": memory_get_v2(conn, memory_id, agent=token)}
        key = arguments.get("key")
        return {"memory": memory_get(conn, key)}

    if tool == "memory_put":
        _require_write(token)
        ttl_days = arguments.get("ttl_days")
        expires_at = arguments.get("expires_at")
        default_ttl_days = config.get("memory", {}).get("default_ttl_days")
        entry = memory_put(
            conn,
            key=arguments.get("key"),
            content=arguments.get("content"),
            tags=arguments.get("tags"),
            metadata=arguments.get("metadata"),
            ttl_days=ttl_days,
            expires_at=expires_at,
            default_ttl_days=default_ttl_days,
        )
        return {"memory": entry}

    if tool == "memory_write":
        _require_write(token)
        entry = memory_write(
            conn,
            content=arguments.get("content"),
            memory_type=arguments.get("memory_type", "context"),
            slot=arguments.get("slot"),
            scope_type=arguments.get("scope_type", "user"),
            scope_id=arguments.get("scope_id"),
            source_agent=arguments.get("source_agent") or token.name,
            source_agent_version=arguments.get("source_agent_version"),
            source_session_id=arguments.get("source_session_id"),
            source_conversation_id=arguments.get("source_conversation_id"),
            source_context=arguments.get("source_context"),
            tags=arguments.get("tags"),
            relations=arguments.get("relations"),
            expires_at=arguments.get("expires_at"),
            sensitivity=arguments.get("sensitivity", "normal"),
            actor=token.name,
            agent=token,
            config=config,
        )
        return {"memory": entry}

    if tool == "memory_query":
        _require_read(token)
        return memory_query_v2(conn, params=arguments, agent=token, config=config)

    if tool == "memory_retract":
        _require_write(token)
        success = memory_retract(
            conn,
            memory_id=arguments.get("id"),
            actor=token.name,
            reason=arguments.get("reason"),
        )
        return {"success": success}

    if tool == "memory_supersede":
        _require_write(token)
        success = memory_supersede(
            conn,
            memory_id=arguments.get("id"),
            superseded_by=arguments.get("superseded_by"),
            actor=token.name,
        )
        return {"success": success}

    if tool == "memory_propose":
        _require_write(token)
        proposal = memory_propose(
            conn,
            proposed_memory=arguments.get("memory"),
            proposed_by=token.name,
            config=config,
            ttl_days=arguments.get("ttl_days"),
        )
        return {"proposal": proposal}

    if tool == "memory_review":
        _require_write(token)
        result = memory_review(
            conn,
            proposal_id=arguments.get("proposal_id"),
            approved=bool(arguments.get("approved")),
            reviewer=token.name,
            config=config,
        )
        return {"result": result}

    if tool == "memory_search":
        _require_read(token)
        query = arguments.get("query", "")
        limit = int(arguments.get("limit", 20))
        return {"results": memory_search(conn, query, limit=limit, agent=token, config=config)}

    if tool == "conflicts_list":
        _require_read(token)
        return {"results": conflicts_list(conn, status=arguments.get("status"))}

    if tool == "conflict_resolve":
        _require_write(token)
        success = conflict_resolve(
            conn,
            conflict_id=arguments.get("conflict_id"),
            resolution=arguments.get("resolution"),
            resolved_by=token.name,
        )
        return {"success": success}

    if tool == "duplicates_list":
        _require_read(token)
        return {"results": duplicates_list(conn, status=arguments.get("status"))}

    if tool == "duplicate_resolve":
        _require_write(token)
        success = duplicate_resolve(
            conn,
            duplicate_id=arguments.get("duplicate_id"),
            resolution=arguments.get("resolution"),
        )
        return {"success": success}

    return None


def _require_read(token) -> None:
    if any(scope in token.scopes for scope in {"memory", "memory.read", "memory.write"}):
        return
    raise ScopeError("Missing scopes: memory.read")


def _require_write(token) -> None:
    if any(scope in token.scopes for scope in {"memory", "memory.write"}):
        return
    raise ScopeError("Missing scopes: memory.write")
