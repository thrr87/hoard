from __future__ import annotations

from pathlib import Path

from hoard.connectors.local_files.connector import LocalFilesConnector
from hoard.core.config import save_config
from hoard.core.db.connection import connect, initialize_db
from hoard.core.ingest.sync import sync_connector
from hoard.core.mcp.stdio import StdioMCPServer


def test_stdio_tools_list_and_search(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "a.md").write_text("Hoard search test one")
    (data_dir / "b.md").write_text("Hoard search test two")

    db_path = tmp_path / "hoard.db"
    config_path = tmp_path / "config.yaml"

    config = {
        "security": {
            "tokens": [
                {
                    "name": "test",
                    "token": "hoard_sk_test",
                    "scopes": ["search", "get", "memory", "sync"],
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
    }

    save_config(config, config_path)

    conn = connect(db_path)
    initialize_db(conn)
    stats = sync_connector(conn, LocalFilesConnector(), config["connectors"]["local_files"])
    assert stats.entities_seen == 2
    conn.close()

    server = StdioMCPServer(config_path)
    init_resp = server._handle_single_message(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {"protocolVersion": "2025-11-25"},
        }
    )
    assert init_resp

    server._handle_single_message({"jsonrpc": "2.0", "method": "notifications/initialized"})

    tools_resp = server._handle_single_message(
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}
    )
    assert tools_resp
    tool_names = {tool["name"] for tool in tools_resp["result"]["tools"]}
    assert "search" in tool_names

    search_resp = server._handle_single_message(
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "search",
                "arguments": {"query": "Hoard", "limit": 1, "token": "hoard_sk_test"},
            },
        }
    )
    assert search_resp
    results = search_resp["result"]["results"]
    assert results
    assert search_resp["result"]["next_cursor"] is not None

    cursor = search_resp["result"]["next_cursor"]
    next_resp = server._handle_single_message(
        {
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": "search",
                "arguments": {"query": "Hoard", "limit": 1, "cursor": cursor, "token": "hoard_sk_test"},
            },
        }
    )
    assert next_resp
    assert next_resp["result"]["results"]

    write_resp = server._handle_single_message(
        {
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {
                "name": "memory_put",
                "arguments": {"key": "k", "content": "v", "token": "hoard_sk_test"},
            },
        }
    )
    assert write_resp["error"]["code"] == -32004
