from __future__ import annotations

from typing import Any, Dict, List

from hoard.core.mcp import tools_admin, tools_data, tools_ingest, tools_memory, tools_orchestrator


MODULES = [
    tools_data,
    tools_memory,
    tools_ingest,
    tools_admin,
    tools_orchestrator,
]


WRITE_TOOLS = set().union(*(module.WRITE_TOOLS for module in MODULES))


def is_write_tool(tool: str) -> bool:
    return tool in WRITE_TOOLS


def dispatch_tool(tool: str, arguments: Dict[str, Any], conn, config: Dict[str, Any], token) -> Dict[str, Any]:
    for module in MODULES:
        response = module.dispatch_tool(tool, arguments, conn, config, token)
        if response is not None:
            return response
    raise ValueError("Unknown tool")


def tool_definitions() -> List[Dict[str, Any]]:
    definitions: List[Dict[str, Any]] = []
    for module in MODULES:
        definitions.extend(module.TOOL_DEFINITIONS)
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
