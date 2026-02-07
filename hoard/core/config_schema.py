from __future__ import annotations

from typing import Any, Dict


def validate_config(config: Dict[str, Any]) -> Dict[str, Any]:
    _require_bool(config, ("mcp", "stdio", "allow_writes"))
    _require_int(config, ("search", "vector_candidate_limit"), minimum=1)
    _require_int(config, ("vectors", "candidate_limit"), minimum=1)
    _require_bool(config, ("vectors", "ann", "enabled"))
    _require_str(config, ("vectors", "ann", "backend"), choices={"hnsw"})
    _require_int(config, ("vectors", "ann", "ef_search"), minimum=1)
    _require_int(config, ("vectors", "ann", "m"), minimum=2)
    _require_int(config, ("vectors", "ann", "ef_construction"), minimum=10)
    _require_int(config, ("write", "limits", "global", "max_memories"), minimum=1)
    _require_int(config, ("write", "limits", "global", "max_content_bytes"), minimum=1)
    _require_int(config, ("write", "query", "vector_candidate_limit"), minimum=1)
    _require_bool(config, ("observability", "metrics_enabled"))
    _require_str(config, ("observability", "log_format"), choices={"json", "plain"})
    _require_str(config, ("observability", "log_level"))
    _require_int(config, ("observability", "wal_checkpoint_interval_seconds"), minimum=0)
    _require_int(config, ("observability", "wal_truncate_idle_seconds"), minimum=0)
    return config


def _value(config: Dict[str, Any], path: tuple[str, ...]) -> Any:
    node: Any = config
    for key in path:
        if not isinstance(node, dict) or key not in node:
            raise ValueError(f"Missing config key: {'.'.join(path)}")
        node = node[key]
    return node


def _require_bool(config: Dict[str, Any], path: tuple[str, ...]) -> None:
    value = _value(config, path)
    if not isinstance(value, bool):
        raise ValueError(f"Config key {'.'.join(path)} must be a boolean")


def _require_str(
    config: Dict[str, Any],
    path: tuple[str, ...],
    *,
    choices: set[str] | None = None,
) -> None:
    value = _value(config, path)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Config key {'.'.join(path)} must be a non-empty string")
    if choices is not None and value not in choices:
        allowed = ", ".join(sorted(choices))
        raise ValueError(f"Config key {'.'.join(path)} must be one of: {allowed}")


def _require_int(config: Dict[str, Any], path: tuple[str, ...], *, minimum: int = 0) -> None:
    value = _value(config, path)
    if not isinstance(value, int):
        raise ValueError(f"Config key {'.'.join(path)} must be an integer")
    if value < minimum:
        raise ValueError(f"Config key {'.'.join(path)} must be >= {minimum}")
