from __future__ import annotations

from pathlib import Path

from hoard.core.db.connection import connect, initialize_db
from hoard.core.mcp.server import MCPRequestHandler
from hoard.core.security.auth import authenticate_token
from hoard.core.security.limits import RateLimiter


def test_http_jsonrpc_invalid_tool_returns_error(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
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

    conn = connect(db_path)
    initialize_db(conn)

    token = authenticate_token("hoard_sk_test", config)
    limiter = RateLimiter(conn, config, enforce=False)

    handler = MCPRequestHandler.__new__(MCPRequestHandler)
    handler.server = type("Server", (), {"config": config})()

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": "nope", "arguments": {}},
    }
    response = MCPRequestHandler._dispatch_jsonrpc(handler, payload, conn, limiter, token)
    assert response["error"]["code"] == -32601

    conn.close()


def test_http_jsonrpc_missing_tool_name_returns_error(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
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

    conn = connect(db_path)
    initialize_db(conn)

    token = authenticate_token("hoard_sk_test", config)
    limiter = RateLimiter(conn, config, enforce=False)

    handler = MCPRequestHandler.__new__(MCPRequestHandler)
    handler.server = type("Server", (), {"config": config})()

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"arguments": {}},
    }
    response = MCPRequestHandler._dispatch_jsonrpc(handler, payload, conn, limiter, token)
    assert response["error"]["code"] == -32602

    conn.close()
