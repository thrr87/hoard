from __future__ import annotations

from typing import Any, Dict, List

from hoard.core.embeddings.model import EmbeddingError, EmbeddingModel
from hoard.core.embeddings.store import build_embeddings
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
from hoard.core.ingest.sync import run_sync
from hoard.core.search.getters import get_chunk, get_entity
from hoard.core.search.service import search_entities
import secrets

from hoard.core.security.agent_tokens import delete_agent, list_agents, register_agent
from hoard.core.security.auth import require_scopes

WRITE_TOOLS = {
    "memory_put",
    "memory_write",
    "memory_retract",
    "memory_supersede",
    "memory_propose",
    "memory_review",
    "conflict_resolve",
    "duplicate_resolve",
    "sync_run",
    "embeddings_build",
    "agent_register",
    "agent_remove",
}


def is_write_tool(tool: str) -> bool:
    return tool in WRITE_TOOLS


def dispatch_tool(tool: str, arguments: Dict[str, Any], conn, config: Dict[str, Any], token) -> Dict[str, Any]:
    allow_sensitive = "sensitive" in token.scopes

    if tool == "search":
        require_scopes(token, ["search"])
        query = arguments.get("query", "")
        limit = int(arguments.get("limit", 20))
        cursor = arguments.get("cursor")
        offset = int(cursor or 0)
        source = arguments.get("source")
        results, next_cursor = search_entities(
            conn,
            query=query,
            config=config,
            limit=limit,
            offset=offset,
            source=source,
            allow_sensitive=allow_sensitive,
        )
        return {"results": results, "next_cursor": next_cursor}

    if tool == "get":
        require_scopes(token, ["get"])
        entity_id = arguments.get("entity_id")
        return {"entity": get_entity(conn, entity_id, allow_sensitive=allow_sensitive)}

    if tool == "get_chunk":
        require_scopes(token, ["get"])
        chunk_id = arguments.get("chunk_id")
        context_chunks = int(arguments.get("context_chunks", 0) or 0)
        return {
            "chunk": get_chunk(
                conn,
                chunk_id,
                allow_sensitive=allow_sensitive,
                context_chunks=context_chunks,
            )
        }

    if tool == "memory_get":
        require_scopes(token, ["memory"])
        memory_id = arguments.get("id")
        if memory_id:
            return {"memory": memory_get_v2(conn, memory_id)}
        key = arguments.get("key")
        return {"memory": memory_get(conn, key)}

    if tool == "memory_put":
        require_scopes(token, ["memory"])
        entry = memory_put(
            conn,
            key=arguments.get("key"),
            content=arguments.get("content"),
            tags=arguments.get("tags"),
            metadata=arguments.get("metadata"),
        )
        return {"memory": entry}

    if tool == "memory_write":
        require_scopes(token, ["memory"])
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
        require_scopes(token, ["memory"])
        return memory_query_v2(conn, params=arguments, agent=token, config=config)

    if tool == "memory_retract":
        require_scopes(token, ["memory"])
        success = memory_retract(
            conn,
            memory_id=arguments.get("id"),
            actor=token.name,
            reason=arguments.get("reason"),
        )
        return {"success": success}

    if tool == "memory_supersede":
        require_scopes(token, ["memory"])
        success = memory_supersede(
            conn,
            memory_id=arguments.get("id"),
            superseded_by=arguments.get("superseded_by"),
            actor=token.name,
        )
        return {"success": success}

    if tool == "memory_propose":
        require_scopes(token, ["memory"])
        proposal = memory_propose(
            conn,
            proposed_memory=arguments.get("memory"),
            proposed_by=token.name,
            config=config,
            ttl_days=arguments.get("ttl_days"),
        )
        return {"proposal": proposal}

    if tool == "memory_review":
        require_scopes(token, ["memory"])
        result = memory_review(
            conn,
            proposal_id=arguments.get("proposal_id"),
            approved=bool(arguments.get("approved")),
            reviewer=token.name,
            config=config,
        )
        return {"result": result}

    if tool == "memory_search":
        require_scopes(token, ["memory"])
        query = arguments.get("query", "")
        limit = int(arguments.get("limit", 20))
        return {"results": memory_search(conn, query, limit=limit)}

    if tool == "conflicts_list":
        require_scopes(token, ["memory"])
        return {"results": conflicts_list(conn, status=arguments.get("status"))}

    if tool == "conflict_resolve":
        require_scopes(token, ["memory"])
        success = conflict_resolve(
            conn,
            conflict_id=arguments.get("conflict_id"),
            resolution=arguments.get("resolution"),
            resolved_by=token.name,
        )
        return {"success": success}

    if tool == "duplicates_list":
        require_scopes(token, ["memory"])
        return {"results": duplicates_list(conn, status=arguments.get("status"))}

    if tool == "duplicate_resolve":
        require_scopes(token, ["memory"])
        success = duplicate_resolve(
            conn,
            duplicate_id=arguments.get("duplicate_id"),
            resolution=arguments.get("resolution"),
        )
        return {"success": success}

    if tool == "sync_status":
        require_scopes(token, ["sync"])
        return _sync_status(conn)

    if tool == "sync_run":
        require_scopes(token, ["sync"])
        result = run_sync(conn, config=config, source=arguments.get("source"))
        return {"result": result}

    if tool == "embeddings_build":
        require_scopes(token, ["sync"])
        vectors_config = config.get("vectors", {})
        model_name = vectors_config.get("model_name", "sentence-transformers/all-MiniLM-L6-v2")
        batch_size = int(vectors_config.get("batch_size", 32))
        source = arguments.get("source")
        try:
            model = EmbeddingModel(model_name)
        except EmbeddingError as exc:
            return {"error": str(exc)}
        total = build_embeddings(conn, model, batch_size=batch_size, source=source)
        return {"total": total}

    if tool == "agent_register":
        require_scopes(token, ["admin"])
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
        require_scopes(token, ["admin"])
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
        require_scopes(token, ["admin"])
        agent_id = arguments.get("agent_id")
        if not agent_id:
            raise ValueError("agent_id is required")
        success = delete_agent(conn, agent_id)
        return {"success": success}

    raise ValueError("Unknown tool")


def tool_definitions() -> List[Dict[str, Any]]:
    return [
        {
            "name": "search",
            "description": "Hybrid search (BM25 + vectors) over indexed chunks.",
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
                        "description": "Maximum number of entities to return.",
                    },
                    "cursor": {
                        "type": "string",
                        "description": "Pagination cursor returned by a previous search.",
                    },
                    "source": {
                        "type": "string",
                        "description": "Optional source filter (e.g., local_files, obsidian).",
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
            "name": "get",
            "description": "Get an entity and all its chunks.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "entity_id": {
                        "type": "string",
                        "description": "Entity ID from search results.",
                    },
                    "token": {
                        "type": "string",
                        "description": "Auth token (if HOARD_TOKEN env not set).",
                    },
                },
                "required": ["entity_id"],
            },
        },
        {
            "name": "get_chunk",
            "description": "Get a single chunk by ID.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "chunk_id": {
                        "type": "string",
                        "description": "Chunk ID from search results.",
                    },
                    "context_chunks": {
                        "type": "integer",
                        "default": 0,
                        "minimum": 0,
                        "maximum": 10,
                        "description": "Number of surrounding chunks to include before/after.",
                    },
                    "token": {
                        "type": "string",
                        "description": "Auth token (if HOARD_TOKEN env not set).",
                    },
                },
                "required": ["chunk_id"],
            },
        },
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
                    "status": {"type": "string"},
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
            "description": "Resolve memory duplicates.",
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
        {
            "name": "sync_status",
            "description": "Get connector sync status.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "token": {
                        "type": "string",
                        "description": "Auth token (if HOARD_TOKEN env not set).",
                    },
                },
                "required": [],
            },
        },
        {
            "name": "sync_run",
            "description": "Run connector sync now.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "source": {"type": "string"},
                    "token": {"type": "string"},
                },
                "required": [],
            },
        },
        {
            "name": "embeddings_build",
            "description": "Build embeddings for indexed content.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "source": {"type": "string"},
                    "token": {"type": "string"},
                },
                "required": [],
            },
        },
        {
            "name": "agent_register",
            "description": "Register a new agent token.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "agent_id": {"type": "string"},
                    "scopes": {"type": "array", "items": {"type": "string"}},
                    "token": {"type": "string"},
                    "overwrite": {"type": "boolean"},
                },
                "required": ["agent_id"],
            },
        },
        {
            "name": "agent_list",
            "description": "List registered agents.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "token": {"type": "string"},
                },
                "required": [],
            },
        },
        {
            "name": "agent_remove",
            "description": "Remove an agent.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "agent_id": {"type": "string"},
                    "token": {"type": "string"},
                },
                "required": ["agent_id"],
            },
        },
    ]


def count_chunks(response: Dict[str, Any]) -> int:
    if not response:
        return 0
    if "results" in response:
        total = 0
        for entity in response["results"] or []:
            total += len(entity.get("chunks", []))
        return total
    if "entity" in response and response["entity"]:
        return len(response["entity"].get("chunks", []))
    if "chunk" in response and response["chunk"]:
        return 1
    return 0


def _sync_status(conn) -> Dict[str, Any]:
    rows = conn.execute(
        """
        SELECT source, COUNT(*) AS count, MAX(synced_at) AS last_sync
        FROM entities
        WHERE tombstoned_at IS NULL
        GROUP BY source
        """
    ).fetchall()
    return {
        "connectors": [
            {"source": row["source"], "entities": row["count"], "last_sync": row["last_sync"]}
            for row in rows
        ]
    }
