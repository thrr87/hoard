from __future__ import annotations

import copy
import warnings
from typing import Any, Dict, List

from hoard.core.mcp import tools_admin, tools_data, tools_ingest, tools_memory, tools_orchestrator


MODULES = [
    tools_data,
    tools_memory,
    tools_ingest,
    tools_admin,
    tools_orchestrator,
]


# Canonical names for underscore-style legacy tools.
LEGACY_TOOL_ALIASES: dict[str, str] = {
    "search": "data.search",
    "get": "data.get",
    "get_chunk": "data.get_chunk",
    "memory_get": "memory.get",
    "memory_put": "memory.put",
    "memory_write": "memory.write",
    "memory_query": "memory.query",
    "memory_retract": "memory.retract",
    "memory_supersede": "memory.supersede",
    "memory_propose": "memory.propose",
    "memory_review": "memory.review",
    "memory_search": "memory.search",
    "conflicts_list": "memory.conflicts.list",
    "conflict_resolve": "memory.conflicts.resolve",
    "duplicates_list": "memory.duplicates.list",
    "duplicate_resolve": "memory.duplicates.resolve",
    "sync": "ingest.sync",
    "sync_status": "ingest.status",
    "sync_run": "ingest.run",
    "embeddings_build": "ingest.embeddings.build",
    "inbox_put": "ingest.inbox.put",
    "agent_register": "admin.agent.register",
    "agent_list": "admin.agent.list",
    "agent_remove": "admin.agent.remove",
}

CANONICAL_TO_LEGACY: dict[str, str] = {v: k for k, v in LEGACY_TOOL_ALIASES.items()}
WRITE_TOOLS = set().union(*(module.WRITE_TOOLS for module in MODULES))
_WARNED_LEGACY_NAMES: set[str] = set()


def normalize_tool_name(tool: str) -> str:
    return CANONICAL_TO_LEGACY.get(tool, tool)


def canonical_tool_name(tool: str) -> str:
    return LEGACY_TOOL_ALIASES.get(tool, tool)


def is_write_tool(tool: str) -> bool:
    return normalize_tool_name(tool) in WRITE_TOOLS


def dispatch_tool(tool: str, arguments: Dict[str, Any], conn, config: Dict[str, Any], token) -> Dict[str, Any]:
    dispatch_name = normalize_tool_name(tool)
    _warn_if_legacy_name(tool)
    for module in MODULES:
        response = module.dispatch_tool(dispatch_name, arguments, conn, config, token)
        if response is not None:
            return response
    raise ValueError("Unknown tool")


def tool_definitions(
    *,
    include_auth_token: bool = True,
    include_legacy_aliases: bool = True,
) -> List[Dict[str, Any]]:
    definitions: List[Dict[str, Any]] = []
    for module in MODULES:
        for raw_definition in module.TOOL_DEFINITIONS:
            definition = copy.deepcopy(raw_definition)
            legacy_name = definition.get("name")
            canonical_name = canonical_tool_name(legacy_name)

            if canonical_name != legacy_name:
                canonical = copy.deepcopy(definition)
                canonical["name"] = canonical_name
                if not include_auth_token:
                    _strip_token_schema(canonical)
                definitions.append(canonical)

                if include_legacy_aliases:
                    if canonical_name not in _existing_aliases(definition):
                        description = definition.get("description", "")
                        definition["description"] = (
                            f"{description} (Legacy alias; prefer `{canonical_name}`.)".strip()
                        )
                    definition["deprecated"] = True
                    if not include_auth_token:
                        _strip_token_schema(definition)
                    definitions.append(definition)
            else:
                if not include_auth_token:
                    _strip_token_schema(definition)
                definitions.append(definition)
    return definitions


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


def _strip_token_schema(definition: Dict[str, Any]) -> None:
    schema = definition.get("inputSchema")
    if not isinstance(schema, dict):
        return
    properties = schema.get("properties")
    if isinstance(properties, dict):
        properties.pop("token", None)
    required = schema.get("required")
    if isinstance(required, list):
        schema["required"] = [value for value in required if value != "token"]


def _warn_if_legacy_name(tool: str) -> None:
    canonical = LEGACY_TOOL_ALIASES.get(tool)
    if canonical is None or tool in _WARNED_LEGACY_NAMES:
        return
    _WARNED_LEGACY_NAMES.add(tool)
    warnings.warn(
        f"Tool `{tool}` is a legacy alias; prefer `{canonical}`.",
        DeprecationWarning,
        stacklevel=3,
    )


def _existing_aliases(definition: Dict[str, Any]) -> set[str]:
    aliases = definition.get("aliases")
    if isinstance(aliases, list):
        return {str(value) for value in aliases}
    return set()

