from __future__ import annotations

import os
import secrets
from pathlib import Path

from hoard.core.config import default_data_path


def server_secret_env_key(config: dict) -> str:
    return config.get("write", {}).get("server_secret_env", "HOARD_SERVER_SECRET")


def server_secret_file_path(config: dict) -> Path:
    raw_path = config.get("write", {}).get("server_secret_file")
    if raw_path:
        return Path(raw_path).expanduser()
    return default_data_path("server.key")


def resolve_server_secret(config: dict) -> str | None:
    env_key = server_secret_env_key(config)
    if env_key:
        env_secret = os.environ.get(env_key, "").strip()
        if env_secret:
            return env_secret

    file_secret = _read_secret_file(server_secret_file_path(config))
    if file_secret:
        return file_secret
    return None


def require_server_secret(config: dict) -> str:
    secret = resolve_server_secret(config)
    if secret:
        return secret
    raise RuntimeError(_missing_secret_message(config))


def ensure_server_secret(config: dict, *, generate: bool) -> tuple[str, str]:
    env_key = server_secret_env_key(config)
    if env_key:
        env_secret = os.environ.get(env_key, "").strip()
        if env_secret:
            return env_secret, "env"

    path = server_secret_file_path(config)
    file_secret = _read_secret_file(path)
    if file_secret:
        _chmod_best_effort(path)
        return file_secret, "file"

    auto_generate = bool(config.get("write", {}).get("auto_generate_server_secret", True))
    if not generate or not auto_generate:
        raise RuntimeError(_missing_secret_message(config))

    secret = secrets.token_hex(32)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(secret + "\n")
    _chmod_best_effort(path)
    return secret, "file"


def _read_secret_file(path: Path) -> str | None:
    if not path.exists():
        return None
    value = path.read_text().strip()
    return value or None


def _chmod_best_effort(path: Path) -> None:
    try:
        os.chmod(path, 0o600)
    except OSError:
        return


def _missing_secret_message(config: dict) -> str:
    env_key = server_secret_env_key(config)
    file_path = server_secret_file_path(config)
    auto_generate = bool(config.get("write", {}).get("auto_generate_server_secret", True))
    message = f"Server secret is not configured. Set {env_key} or create {file_path}."
    if not auto_generate:
        message += " Auto-generation is disabled in write.auto_generate_server_secret."
    return message
