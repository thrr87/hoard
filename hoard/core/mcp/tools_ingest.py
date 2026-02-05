from __future__ import annotations

from typing import Any, Dict, List

from hoard.core.embeddings.model import EmbeddingError, EmbeddingModel
from hoard.core.embeddings.store import build_embeddings
from hoard.core.ingest.inbox import write_inbox_entry
from hoard.core.ingest.sync import run_sync
from hoard.core.security.errors import ScopeError
from hoard.core.sync.service import sync_with_lock


TOOL_DEFINITIONS: List[Dict[str, Any]] = [
    {
        "name": "sync",
        "description": "Run sync for enabled connectors.",
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
        "name": "sync_status",
        "description": "Get connector sync status.",
        "inputSchema": {
            "type": "object",
            "properties": {"token": {"type": "string"}},
            "required": [],
        },
    },
    {
        "name": "sync_run",
        "description": "Run sync without lock (advanced).",
        "inputSchema": {
            "type": "object",
            "properties": {"source": {"type": "string"}, "token": {"type": "string"}},
            "required": [],
        },
    },
    {
        "name": "embeddings_build",
        "description": "Build embeddings for indexed chunks.",
        "inputSchema": {
            "type": "object",
            "properties": {"source": {"type": "string"}, "token": {"type": "string"}},
            "required": [],
        },
    },
    {
        "name": "inbox_put",
        "description": "Write content into the agent inbox.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "content": {"type": "string"},
                "title": {"type": "string"},
                "tags": {"type": "array", "items": {"type": "string"}},
                "metadata": {"type": "object"},
                "filename": {"type": "string"},
                "extension": {"type": "string"},
                "sync_immediately": {"type": "boolean"},
                "token": {"type": "string"},
            },
            "required": ["content"],
        },
    },
]


WRITE_TOOLS = {"sync", "sync_run", "embeddings_build", "inbox_put"}


def dispatch_tool(tool: str, arguments: Dict[str, Any], conn, config: Dict[str, Any], token):
    if tool == "sync":
        _require_sync(token)
        source = arguments.get("source")
        return sync_with_lock(conn, config, source=source)

    if tool == "sync_status":
        _require_sync(token)
        return _sync_status(conn)

    if tool == "sync_run":
        _require_sync(token)
        result = run_sync(conn, config=config, source=arguments.get("source"))
        return {"result": result}

    if tool == "embeddings_build":
        _require_sync(token)
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

    if tool == "inbox_put":
        _require_ingest(token)
        content = arguments.get("content")
        if content is None:
            raise ValueError("Missing content")
        path = write_inbox_entry(
            config,
            content=content,
            title=arguments.get("title"),
            tags=arguments.get("tags"),
            metadata=arguments.get("metadata"),
            filename=arguments.get("filename"),
            extension=arguments.get("extension", ".md"),
        )

        sync_immediately = bool(arguments.get("sync_immediately", False))
        if sync_immediately or not config.get("sync", {}).get("watcher_enabled", False):
            sync_with_lock(conn, config, source="inbox")

        return {"path": str(path)}

    return None


def _require_sync(token) -> None:
    if any(scope in token.scopes for scope in {"sync", "system.status", "system.sync"}):
        return
    raise ScopeError("Missing scopes: sync")


def _require_ingest(token) -> None:
    if any(scope in token.scopes for scope in {"ingest"}):
        return
    raise ScopeError("Missing scopes: ingest")


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
