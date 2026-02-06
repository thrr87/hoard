from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from pathlib import Path

from hoard.connectors.local_files.connector import LocalFilesConnector
from hoard.core.config import save_config
from hoard.core.db.connection import connect, initialize_db
from hoard.core.ingest.sync import sync_connector


def _call_mcp(url: str, token: str | None, method: str, params: dict) -> tuple[int, dict]:
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    data = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=headers)
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.status, json.loads(resp.read())


def test_http_mcp_tools_list_and_search(tmp_path: Path, mcp_server) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "note.md").write_text("Hoard http search")

    db_path = tmp_path / "hoard.db"
    config_path = tmp_path / "config.yaml"
    config = {
        "security": {
            "tokens": [
                {
                    "name": "test",
                    "token": "hoard_sk_test",
                    "scopes": ["search", "get", "memory", "sync", "ingest"],
                }
            ]
        },
        "storage": {"db_path": str(db_path)},
        "connectors": {
            "local_files": {
                "enabled": True,
                "paths": [str(data_dir)],
                "include_extensions": [".md"],
                "chunk_max_tokens": 50,
                "chunk_overlap_tokens": 0,
            }
        },
        "vectors": {"enabled": False},
    }
    save_config(config, config_path)

    conn = connect(db_path)
    initialize_db(conn)
    sync_connector(conn, LocalFilesConnector(), config["connectors"]["local_files"])
    conn.close()

    url = mcp_server(config_path)
    try:
        status, resp = _call_mcp(url, "hoard_sk_test", "tools/list", {})
        assert status == 200
        assert resp["result"]["tools"]

        _, search = _call_mcp(
            url,
            "hoard_sk_test",
            "tools/call",
            {"name": "search", "arguments": {"query": "Hoard", "limit": 1}},
        )
        content = json.loads(search["result"]["content"][0]["text"])
        assert content["results"]
    finally:
        pass


def test_http_mcp_auth_error(tmp_path: Path, mcp_server) -> None:
    db_path = tmp_path / "hoard.db"
    config_path = tmp_path / "config.yaml"
    config = {
        "security": {"tokens": [{"name": "test", "token": "hoard_sk_test", "scopes": ["search"]}]},
        "storage": {"db_path": str(db_path)},
    }
    save_config(config, config_path)

    url = mcp_server(config_path)
    try:
        payload = {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(req, timeout=5)
        except urllib.error.HTTPError as exc:
            assert exc.code == 401
    finally:
        pass


def test_http_mcp_admin_auth_uses_file_secret(tmp_path: Path, mcp_server, monkeypatch) -> None:
    monkeypatch.delenv("HOARD_SERVER_SECRET", raising=False)
    db_path = tmp_path / "hoard.db"
    secret_file = tmp_path / "server.key"
    secret_file.write_text("file-secret\n")
    config_path = tmp_path / "config.yaml"
    config = {
        "storage": {"db_path": str(db_path)},
        "write": {"server_secret_file": str(secret_file)},
    }
    save_config(config, config_path)

    url = mcp_server(config_path)
    status, resp = _call_mcp(url, "file-secret", "tools/list", {})
    assert status == 200
    assert resp["result"]["tools"]


def test_http_mcp_config_token_still_works_with_file_secret(tmp_path: Path, mcp_server, monkeypatch) -> None:
    monkeypatch.delenv("HOARD_SERVER_SECRET", raising=False)
    db_path = tmp_path / "hoard.db"
    secret_file = tmp_path / "server.key"
    secret_file.write_text("file-secret\n")
    config_path = tmp_path / "config.yaml"
    config = {
        "security": {
            "tokens": [
                {
                    "name": "test",
                    "token": "hoard_sk_test",
                    "scopes": ["search", "get", "memory", "sync", "ingest"],
                }
            ]
        },
        "storage": {"db_path": str(db_path)},
        "write": {"server_secret_file": str(secret_file)},
    }
    save_config(config, config_path)

    url = mcp_server(config_path)
    status, resp = _call_mcp(url, "hoard_sk_test", "tools/list", {})
    assert status == 200
    assert resp["result"]["tools"]


def test_http_mcp_inbox_put_and_sync(tmp_path: Path, mcp_server) -> None:
    original_home = os.environ.get("HOME")
    home_dir = tmp_path / "home"
    home_dir.mkdir(parents=True, exist_ok=True)
    os.environ["HOME"] = str(home_dir)

    try:
        inbox_dir = tmp_path / "inbox"
        inbox_dir.mkdir()

        db_path = tmp_path / "hoard.db"
        config_path = tmp_path / "config.yaml"
        config = {
            "security": {
                "tokens": [
                    {
                        "name": "test",
                        "token": "hoard_sk_test",
                        "scopes": ["search", "get", "memory", "sync", "ingest"],
                    }
                ]
            },
            "storage": {"db_path": str(db_path)},
            "connectors": {
                "inbox": {
                    "enabled": True,
                    "path": str(inbox_dir),
                    "include_extensions": [".md"],
                    "chunk_max_tokens": 50,
                    "chunk_overlap_tokens": 0,
                }
            },
            "sync": {"watcher_enabled": False},
            "vectors": {"enabled": False},
        }
        save_config(config, config_path)

        url = mcp_server(config_path)
        try:
            _, inbox_resp = _call_mcp(
                url,
                "hoard_sk_test",
                "tools/call",
                {"name": "inbox_put", "arguments": {"content": "Inbox HTTP content"}},
            )
            inbox_content = json.loads(inbox_resp["result"]["content"][0]["text"])
            assert "path" in inbox_content

            _, search = _call_mcp(
                url,
                "hoard_sk_test",
                "tools/call",
                {"name": "search", "arguments": {"query": "Inbox HTTP", "limit": 1}},
            )
            search_content = json.loads(search["result"]["content"][0]["text"])
            assert search_content["results"]
        finally:
            pass
    finally:
        if original_home is None:
            os.environ.pop("HOME", None)
        else:
            os.environ["HOME"] = original_home
