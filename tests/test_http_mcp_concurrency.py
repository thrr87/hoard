from __future__ import annotations

import json
import threading
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


def test_http_mcp_concurrent_search(tmp_path: Path, mcp_server) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "note.md").write_text("Hoard concurrency test")

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

    errors: list[str] = []
    lock = threading.Lock()

    def worker() -> None:
        try:
            for _ in range(3):
                _, resp = _call_mcp(
                    url,
                    "hoard_sk_test",
                    "tools/call",
                    {"name": "search", "arguments": {"query": "Hoard", "limit": 1}},
                )
                content = json.loads(resp["result"]["content"][0]["text"])
                assert content["results"]
        except Exception as exc:  # pragma: no cover - diagnostic
            with lock:
                errors.append(str(exc))

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors, f"Concurrent search errors: {errors}"
