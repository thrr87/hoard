from __future__ import annotations

from typing import Any, Dict, List, Optional

from hoard.core.memory.store import memory_get_by_id
from hoard.core.memory.v2.store import memory_get as memory_get_v2
from hoard.core.mcp.scopes import has_any_scope, require_any_scope
from hoard.core.search.getters import get_chunk, get_entity
from hoard.core.search.service import search_entities


TOOL_DEFINITIONS: List[Dict[str, Any]] = [
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
                "types": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Result types to include (entity, memory).",
                },
                "include_memory": {
                    "type": "boolean",
                    "description": "Deprecated. Use types instead. True includes memory results.",
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
]


WRITE_TOOLS: set[str] = set()


def dispatch_tool(tool: str, arguments: Dict[str, Any], conn, config: Dict[str, Any], token):
    if tool == "search":
        require_any_scope(token, {"search", "data.search"})
        query = arguments.get("query", "")
        limit = int(arguments.get("limit", 20))
        cursor = arguments.get("cursor")
        offset = int(cursor or 0)
        source = arguments.get("source")
        include_memory = arguments.get("include_memory")
        types = arguments.get("types")
        if types is None:
            if include_memory is None:
                types = ["entity", "memory"]
            else:
                types = ["entity", "memory"] if bool(include_memory) else ["entity"]
        elif isinstance(types, str):
            types = [value.strip() for value in types.split(",") if value.strip()]

        if not _has_memory_scope(token) and isinstance(types, list):
            types = [value for value in types if value != "memory"]

        results, next_cursor = search_entities(
            conn,
            query=query,
            config=config,
            limit=limit,
            offset=offset,
            source=source,
            allow_sensitive=token.can_access_sensitive,
            types=types if isinstance(types, list) else None,
            agent=token,
        )
        return {"results": results, "next_cursor": next_cursor}

    if tool == "get":
        require_any_scope(token, {"get", "data.get"})
        entity_id = arguments.get("entity_id")
        entity = get_entity(conn, entity_id, allow_sensitive=token.can_access_sensitive)
        if entity is not None:
            return {"entity": entity}

        if _has_memory_scope(token):
            memory_entry = memory_get_v2(conn, entity_id, agent=token)
            if memory_entry:
                return {"entity": _memory_to_entity(memory_entry)}

            legacy = memory_get_by_id(conn, entity_id)
            if legacy:
                return {"entity": _legacy_memory_to_entity(legacy)}

        return {"entity": None}

    if tool == "get_chunk":
        require_any_scope(token, {"get", "data.get"})
        chunk_id = arguments.get("chunk_id")
        context_chunks = int(arguments.get("context_chunks", 0) or 0)
        return {
            "chunk": get_chunk(
                conn,
                chunk_id,
                allow_sensitive=token.can_access_sensitive,
                context_chunks=context_chunks,
            )
        }

    return None


def _has_memory_scope(token) -> bool:
    return has_any_scope(token, {"memory", "memory.read", "memory.write"})


def _memory_to_entity(entry: Dict[str, Any]) -> Dict[str, Any]:
    title = entry.get("slot") or entry.get("memory_type") or entry.get("id")
    return {
        "result_type": "memory",
        "entity_id": entry["id"],
        "entity_title": title,
        "source": "memory",
        "uri": None,
        "updated_at": entry.get("created_at"),
        "memory_type": entry.get("memory_type"),
        "scope_type": entry.get("scope_type"),
        "scope_id": entry.get("scope_id"),
        "tags": entry.get("tags", []),
        "chunks": [
            {
                "chunk_id": entry["id"],
                "chunk_index": 0,
                "content": entry["content"],
                "char_offset_start": None,
                "char_offset_end": None,
                "chunk_type": "memory",
            }
        ],
    }


def _legacy_memory_to_entity(entry: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "result_type": "memory",
        "entity_id": entry["id"],
        "entity_title": entry["key"],
        "source": "memory",
        "uri": None,
        "updated_at": entry.get("updated_at"),
        "memory_key": entry["key"],
        "tags": entry.get("tags", []),
        "metadata": entry.get("metadata"),
        "chunks": [
            {
                "chunk_id": entry["id"],
                "chunk_index": 0,
                "content": entry["content"],
                "char_offset_start": None,
                "char_offset_end": None,
                "chunk_type": "memory",
            }
        ],
    }
