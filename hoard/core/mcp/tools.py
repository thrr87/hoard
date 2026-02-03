from __future__ import annotations

from typing import Any, Dict, List

from hoard.core.memory.store import memory_get, memory_put, memory_search
from hoard.core.search.getters import get_chunk, get_entity
from hoard.core.search.service import search_entities
from hoard.core.security.auth import require_scopes


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

    if tool == "memory_search":
        require_scopes(token, ["memory"])
        query = arguments.get("query", "")
        limit = int(arguments.get("limit", 20))
        return {"results": memory_search(conn, query, limit=limit)}

    if tool == "sync_status":
        require_scopes(token, ["sync"])
        return _sync_status(conn)

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
            "description": "Fetch a memory entry by key.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "key": {
                        "type": "string",
                        "description": "Memory key.",
                    },
                    "token": {
                        "type": "string",
                        "description": "Auth token (if HOARD_TOKEN env not set).",
                    },
                },
                "required": ["key"],
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
