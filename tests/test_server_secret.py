from __future__ import annotations

from pathlib import Path

import pytest

from hoard.core.security.server_secret import (
    ensure_server_secret,
    require_server_secret,
    resolve_server_secret,
)


def test_resolve_prefers_env_over_file(tmp_path: Path, monkeypatch) -> None:
    secret_file = tmp_path / "server.key"
    secret_file.write_text("file-secret\n")
    monkeypatch.setenv("HOARD_SERVER_SECRET", "env-secret")

    config = {
        "write": {
            "server_secret_env": "HOARD_SERVER_SECRET",
            "server_secret_file": str(secret_file),
        }
    }

    assert resolve_server_secret(config) == "env-secret"
    secret, source = ensure_server_secret(config, generate=False)
    assert secret == "env-secret"
    assert source == "env"


def test_resolve_uses_file_when_env_missing(tmp_path: Path, monkeypatch) -> None:
    secret_file = tmp_path / "server.key"
    secret_file.write_text("file-secret\n")
    monkeypatch.delenv("HOARD_SERVER_SECRET", raising=False)

    config = {
        "write": {
            "server_secret_env": "HOARD_SERVER_SECRET",
            "server_secret_file": str(secret_file),
        }
    }

    assert resolve_server_secret(config) == "file-secret"
    secret, source = ensure_server_secret(config, generate=False)
    assert secret == "file-secret"
    assert source == "file"


def test_ensure_generates_file_with_secure_permissions(tmp_path: Path, monkeypatch) -> None:
    secret_file = tmp_path / "server.key"
    monkeypatch.delenv("HOARD_SERVER_SECRET", raising=False)

    config = {
        "write": {
            "server_secret_env": "HOARD_SERVER_SECRET",
            "server_secret_file": str(secret_file),
            "auto_generate_server_secret": True,
        }
    }

    secret, source = ensure_server_secret(config, generate=True)
    assert source == "file"
    assert secret_file.exists()
    assert secret_file.read_text().strip() == secret
    mode = secret_file.stat().st_mode & 0o777
    assert mode == 0o600


def test_missing_secret_without_generation_raises(tmp_path: Path, monkeypatch) -> None:
    secret_file = tmp_path / "server.key"
    monkeypatch.delenv("HOARD_SERVER_SECRET", raising=False)

    config = {
        "write": {
            "server_secret_env": "HOARD_SERVER_SECRET",
            "server_secret_file": str(secret_file),
            "auto_generate_server_secret": False,
        }
    }

    with pytest.raises(RuntimeError, match="Server secret is not configured"):
        ensure_server_secret(config, generate=True)

    with pytest.raises(RuntimeError, match="Server secret is not configured"):
        require_server_secret(config)
