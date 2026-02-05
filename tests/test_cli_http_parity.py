from __future__ import annotations

import json
import urllib.request
from pathlib import Path

from click.testing import CliRunner

from hoard.cli.main import cli
from hoard.core.config import save_config
from hoard.core.db.connection import connect, initialize_db


def _call_mcp(url: str, token: str | None, method: str, params: dict) -> tuple[int, dict]:
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    data = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=headers)
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.status, json.loads(resp.read())


def test_cli_write_http_read(tmp_path: Path, mcp_server) -> None:
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
    }
    save_config(config, config_path)

    conn = connect(db_path)
    initialize_db(conn)
    conn.close()

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["memory", "put", "shared_key", "from cli", "--config", str(config_path)],
    )
    assert result.exit_code == 0

    url = mcp_server(config_path)
    status, resp = _call_mcp(
        url,
        "hoard_sk_test",
        "tools/call",
        {"name": "memory_get", "arguments": {"key": "shared_key"}},
    )
    assert status == 200
    content = json.loads(resp["result"]["content"][0]["text"])
    assert content["memory"]["content"] == "from cli"
