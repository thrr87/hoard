from __future__ import annotations

import copy
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import yaml

from hoard.core.config_schema import validate_config

DEFAULT_CONFIG = {
    "security": {
        "tokens": [],
        "rate_limits": {
            "search_requests_per_minute": 30,
            "get_requests_per_minute": 60,
            "chunks_returned_per_hour": 5000,
            "bytes_returned_per_hour": 50_000_000,
        },
    },
    "connectors": {
        "local_files": {
            "enabled": True,
            "paths": [],
            "include_extensions": [".md", ".txt", ".csv", ".json", ".yaml", ".rst"],
            "chunk_max_tokens": 400,
            "chunk_overlap_tokens": 50,
        },
        "inbox": {
            "enabled": False,
            "path": "",
            "include_extensions": [".md", ".txt", ".csv", ".json", ".yaml", ".rst"],
            "chunk_max_tokens": 400,
            "chunk_overlap_tokens": 50,
        },
        "obsidian": {
            "enabled": False,
            "vault_path": "~/Notes",
            "chunk_max_tokens": 400,
            "chunk_overlap_tokens": 50,
        },
        "bookmarks_chrome": {
            "enabled": False,
            "bookmark_path": "",
        },
        "bookmarks_firefox": {
            "enabled": False,
            "places_path": "",
        },
        "notion_export": {
            "enabled": False,
            "export_path": "",
            "include_databases": True,
            "include_csv_databases": True,
            "schema_sample_rows": 200,
            "max_schema_tags": 200,
            "chunk_max_tokens": 400,
            "chunk_overlap_tokens": 50,
        },
    },
    "sync": {
        "interval_minutes": 15,
        "watcher_enabled": False,
        "watcher_debounce_seconds": 2,
    },
    "memory": {
        "default_ttl_days": 30,
        "prune_on_sync": True,
    },
    "search": {
        "rrf_k": 60,
        "max_chunks_per_entity": 3,
        "vector_candidate_limit": 2000,
    },
    "server": {
        "host": "127.0.0.1",
        "port": 19850,
        "allow_remote": False,
    },
    "vectors": {
        "enabled": False,
        "model_name": "sentence-transformers/all-MiniLM-L6-v2",
        "batch_size": 32,
        "prefilter_limit": 1000,
        "candidate_limit": 2000,
        "ann": {
            "enabled": False,
            "backend": "hnsw",
            "ef_search": 64,
            "m": 16,
            "ef_construction": 200,
        },
    },
    "write": {
        "enabled": True,
        "server_secret_env": "HOARD_SERVER_SECRET",
        "server_secret_file": "~/.hoard/server.key",
        "auto_generate_server_secret": True,
        "database": {
            "busy_timeout_ms": 5000,
            "lock_timeout_ms": 30000,
            "retry_budget_ms": 30000,
            "retry_backoff_ms": 50,
        },
        "slots": {
            "pattern": "^(pref|fact|ctx|decision|event):[a-z0-9_]+(\\.[a-z0-9_]+){0,3}$",
            "prefixes": ["pref", "fact", "ctx", "decision", "event"],
            "on_invalid": "reject",
        },
        "embeddings": {
            "enabled": True,
            "active_model": {
                "name": "sentence-transformers/all-MiniLM-L6-v2",
                "version": "2.0.0",
                "dimensions": 384,
            },
            "format": {
                "dtype": "float32",
                "byte_order": "little",
                "normalization": "l2",
            },
        },
        "proposals": {
            "default_ttl_days": 7,
            "max_ttl_days": 30,
        },
        "limits": {
            "global": {"max_memories": 50000, "max_content_bytes": 262144},
            "per_agent": {"max_writes_per_hour": 100},
            "retention": {
                "default_ttl_days": 365,
                "min_confidence_to_keep": 0.2,
                "unused_decay_after_days": 90,
            },
        },
        "worker": {
            "mode": "process",
            "poll_interval_ms": 1000,
            "job_timeout_seconds": 60,
            "lease_duration_seconds": 60,
            "heartbeat_interval_seconds": 30,
            "max_retries": 3,
        },
        "nli": {
            "model": "cross-encoder/nli-deberta-v3-small",
            "top_k": 5,
            "contradiction_threshold": 0.7,
        },
        "duplicates": {
            "similarity_threshold": 0.85,
        },
        "sensitivity": {
            "sensitive_max_ttl_days": 90,
            "restricted_max_ttl_days": 30,
        },
        "query": {
            "hybrid_weight_vector": 0.6,
            "hybrid_weight_fts": 0.4,
            "slot_match_bonus": 0.1,
            "slot_only_baseline": 0.5,
            "union_multiplier": 2,
            "vector_candidate_limit": 2000,
        },
    },
    "mcp": {
        "stdio": {
            "allow_writes": False,
        }
    },
    "observability": {
        "log_format": "json",
        "log_level": "INFO",
        "metrics_enabled": True,
        "wal_checkpoint_interval_seconds": 60,
        "wal_truncate_idle_seconds": 300,
    },
    "artifacts": {
        "blob_path": "~/.hoard/artifacts",
        "inline_max_bytes": 262_144,
        "retention_days": 30,
    },
    "orchestrator": {
        "registration_token_env": "HOARD_REGISTRATION_TOKEN",
        "fallback_max_bytes": 10_485_760,
        "default_scopes": [
            "agent.self",
            "data.search",
            "data.get",
            "memory.read",
            "task.claim",
            "task.execute",
            "artifact.read",
            "artifact.write",
            "event.read",
            "cost.write",
        ],
    },
    "cost": {
        "budgets": {
            "per_agent": {"default": 5.0},
            "per_workflow": {"default": 50.0},
            "global": {"daily": 50.0, "monthly": 500.0},
        }
    },
    "storage": {
        "db_path": "~/.hoard/hoard.db",
    },
}

HOARD_DATA_DIR_ENV = "HOARD_DATA_DIR"
_DEFAULT_RELATIVE_DB_PATH = "hoard.db"
_DEFAULT_RELATIVE_SECRET_PATH = "server.key"
_DEFAULT_RELATIVE_ARTIFACTS_PATH = "artifacts"
_DEFAULT_RELATIVE_CONFIG_PATH = "config.yaml"


@dataclass
class ConfigPaths:
    config_path: Path
    db_path: Path


def get_data_dir() -> Path:
    raw = os.environ.get(HOARD_DATA_DIR_ENV, "~/.hoard")
    return Path(raw).expanduser()


def default_data_path(*parts: str) -> Path:
    return get_data_dir().joinpath(*parts)


def get_default_config_path() -> Path:
    return default_data_path(_DEFAULT_RELATIVE_CONFIG_PATH)


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = dict(base)
    for key, value in override.items():
        if (
            key in merged
            and isinstance(merged[key], dict)
            and isinstance(value, dict)
        ):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _expand_path(value: str) -> str:
    if not value:
        return value
    return str(Path(value).expanduser())


def _expand_config_paths(config: Dict[str, Any]) -> Dict[str, Any]:
    storage = config.get("storage", {})
    if "db_path" in storage:
        storage["db_path"] = _expand_path(storage["db_path"])

    artifacts = config.get("artifacts", {})
    if "blob_path" in artifacts:
        artifacts["blob_path"] = _expand_path(artifacts["blob_path"])

    write_cfg = config.get("write", {})
    if "server_secret_file" in write_cfg:
        write_cfg["server_secret_file"] = _expand_path(write_cfg["server_secret_file"])

    connectors = config.get("connectors", {})
    for connector in connectors.values():
        if "paths" in connector and isinstance(connector["paths"], list):
            connector["paths"] = [_expand_path(p) for p in connector["paths"]]
        if "path" in connector:
            connector["path"] = _expand_path(connector["path"])
        if "vault_path" in connector:
            connector["vault_path"] = _expand_path(connector["vault_path"])
        if "export_path" in connector:
            connector["export_path"] = _expand_path(connector["export_path"])
        if "bookmark_path" in connector:
            connector["bookmark_path"] = _expand_path(connector["bookmark_path"])
        if "places_path" in connector:
            connector["places_path"] = _expand_path(connector["places_path"])

    return config


def _path_is_explicit(raw: Dict[str, Any], keys: tuple[str, ...]) -> bool:
    node: Any = raw
    for key in keys:
        if not isinstance(node, dict) or key not in node:
            return False
        node = node[key]
    return bool(node)


def _apply_data_dir_defaults(config: Dict[str, Any], raw: Dict[str, Any]) -> Dict[str, Any]:
    data_dir = get_data_dir()

    if not _path_is_explicit(raw, ("storage", "db_path")):
        config.setdefault("storage", {})["db_path"] = str(data_dir / _DEFAULT_RELATIVE_DB_PATH)

    if not _path_is_explicit(raw, ("write", "server_secret_file")):
        config.setdefault("write", {})["server_secret_file"] = str(data_dir / _DEFAULT_RELATIVE_SECRET_PATH)

    if not _path_is_explicit(raw, ("artifacts", "blob_path")):
        config.setdefault("artifacts", {})["blob_path"] = str(data_dir / _DEFAULT_RELATIVE_ARTIFACTS_PATH)

    return config


def load_config(path: Path | None = None) -> Dict[str, Any]:
    config_path = path or get_default_config_path()
    if not config_path.exists():
        base = copy.deepcopy(DEFAULT_CONFIG)
        candidate = _expand_config_paths(_apply_data_dir_defaults(base, {}))
        try:
            return validate_config(candidate)
        except ValueError as exc:
            raise ValueError(f"Invalid config at {config_path}: {exc}") from exc

    data = yaml.safe_load(config_path.read_text()) or {}
    merged = _deep_merge(copy.deepcopy(DEFAULT_CONFIG), data)
    merged = _apply_data_dir_defaults(merged, data)
    candidate = _expand_config_paths(merged)
    try:
        return validate_config(candidate)
    except ValueError as exc:
        raise ValueError(f"Invalid config at {config_path}: {exc}") from exc


def ensure_config_file(path: Path | None = None) -> Path:
    config_path = path or get_default_config_path()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    if not config_path.exists():
        base = copy.deepcopy(DEFAULT_CONFIG)
        config = _apply_data_dir_defaults(base, {})
        save_config(config, config_path)
    return config_path


def save_config(config: Dict[str, Any], path: Path | None = None) -> Path:
    config_path = path or get_default_config_path()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    data = yaml.safe_dump(config, sort_keys=False)
    fd, tmp = tempfile.mkstemp(dir=config_path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, config_path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return config_path


def resolve_paths(config: Dict[str, Any], path: Path | None = None) -> ConfigPaths:
    config_path = path or get_default_config_path()
    db_path_value = config.get("storage", {}).get("db_path")
    if db_path_value:
        db_path = Path(db_path_value).expanduser()
    else:
        db_path = default_data_path(_DEFAULT_RELATIVE_DB_PATH)
    return ConfigPaths(config_path=config_path, db_path=db_path)
