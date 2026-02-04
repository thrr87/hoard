from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Dict

import click

from hoard import __version__
from hoard.core.config import load_config, resolve_paths
from hoard.core.db.connection import connect
from hoard.migrations import get_current_version, get_migrations, migrate
from hoard.core.mcp.tools import count_chunks, dispatch_tool, tool_definitions
from hoard.core.security.audit import log_access
from hoard.core.security.auth import AuthError, ScopeError, authenticate_token
from hoard.core.security.limits import RateLimitError, RateLimiter

SUPPORTED_PROTOCOL_VERSIONS = ["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"]


class MCPRequestHandler(BaseHTTPRequestHandler):
    server_version = "HoardMCP/0.1"

    def do_POST(self) -> None:  # noqa: N802
        try:
            if self.path == "/mcp":
                self._handle_jsonrpc()
            else:
                self._handle_custom_http()
        except Exception as exc:
            self._write_json(500, {"error": str(exc)})

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/sync_status":
            self._handle_custom_http()
            return
        self._write_json(404, {"error": "Not found"})

    def _handle_custom_http(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode("utf-8") if length else "{}"
        payload = json.loads(body) if body else {}

        token = None
        tool = self._tool_name()
        conn = connect(self.server.db_path)
        limiter = RateLimiter(conn, self.server.config, enforce=True)

        try:
            token = self._authenticate()
            limiter.check_request(token.name, tool)
            response = dispatch_tool(tool, payload, conn, self.server.config, token)
            response_bytes = json.dumps(response).encode("utf-8")
            limiter.check_quota(token.name, count_chunks(response), len(response_bytes))
            log_access(
                conn,
                tool=tool,
                success=True,
                token_name=token.name,
                scope=None,
                chunks_returned=count_chunks(response),
                bytes_returned=len(response_bytes),
            )
            self._write_json(200, response)
        except AuthError as exc:
            log_access(conn, tool=tool, success=False, token_name=None)
            self._write_json(401, {"error": str(exc)})
        except ScopeError as exc:
            log_access(conn, tool=tool, success=False, token_name=getattr(token, "name", None))
            self._write_json(403, {"error": str(exc)})
        except RateLimitError as exc:
            log_access(conn, tool=tool, success=False, token_name=getattr(token, "name", None))
            self._write_json(429, {"error": str(exc)})
        finally:
            conn.close()

    def _handle_jsonrpc(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode("utf-8") if length else "{}"
        payload = json.loads(body) if body else {}

        conn = connect(self.server.db_path)
        limiter = RateLimiter(conn, self.server.config, enforce=True)

        try:
            token = self._authenticate()
            response = self._dispatch_jsonrpc(payload, conn, limiter, token)
            self._write_json(200, response)
        except AuthError as exc:
            self._write_json(401, _jsonrpc_error(payload, -32001, str(exc)))
        except ScopeError as exc:
            self._write_json(403, _jsonrpc_error(payload, -32002, str(exc)))
        except RateLimitError as exc:
            self._write_json(429, _jsonrpc_error(payload, -32003, str(exc)))
        finally:
            conn.close()

    def _dispatch_jsonrpc(self, payload: Dict[str, Any], conn, limiter, token):
        if not isinstance(payload, dict):
            return _jsonrpc_error(payload, -32600, "Invalid Request")

        method = payload.get("method")
        params = payload.get("params") or {}
        msg_id = payload.get("id")

        if method == "initialize":
            requested = params.get("protocolVersion")
            version = _negotiate_version(requested)
            return {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {
                    "protocolVersion": version,
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {"name": "hoard", "version": __version__},
                },
            }

        if method == "tools/list":
            return {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {"tools": tool_definitions(), "nextCursor": None},
            }

        if method == "tools/call":
            tool_name = params.get("name")
            arguments = params.get("arguments") or {}
            limiter.check_request(token.name, tool_name)
            response = dispatch_tool(tool_name, arguments, conn, self.server.config, token)
            response_bytes = json.dumps(response).encode("utf-8")
            limiter.check_quota(token.name, count_chunks(response), len(response_bytes))
            log_access(
                conn,
                tool=tool_name,
                success=True,
                token_name=token.name,
                chunks_returned=count_chunks(response),
                bytes_returned=len(response_bytes),
            )
            return {"jsonrpc": "2.0", "id": msg_id, "result": response}

        return _jsonrpc_error(payload, -32601, "Method not found")

    def _tool_name(self) -> str:
        return self.path.lstrip("/")

    def _authenticate(self):
        header = self.headers.get("Authorization", "")
        if not header.startswith("Bearer "):
            raise AuthError("Missing bearer token")
        token_value = header.split(" ", 1)[1]
        return authenticate_token(token_value, self.server.config)

    def _write_json(self, status: int, payload: Dict[str, Any]) -> None:
        response = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


class MCPServer(HTTPServer):
    def __init__(self, server_address, RequestHandlerClass, config_path: Path | None) -> None:
        self.config = load_config(config_path)
        paths = resolve_paths(self.config, config_path)
        self.db_path = paths.db_path
        super().__init__(server_address, RequestHandlerClass)


def run_server(
    host: str = "127.0.0.1",
    port: int = 19850,
    config_path: Path | None = None,
    no_migrate: bool = False,
) -> None:
    server = MCPServer((host, port), MCPRequestHandler, config_path)

    conn = connect(server.db_path)
    try:
        if no_migrate:
            migrations = get_migrations()
            latest = max(migrations.keys()) if migrations else 0
            current = get_current_version(conn)
            if current < latest:
                click.echo(f"⚠️  Schema migrations pending (v{current} → v{latest})")
                click.echo("   Run 'hoard db migrate' or restart without --no-migrate")
        else:
            migrate(conn, app_version=__version__)
    finally:
        conn.close()

    server.serve_forever()


def _jsonrpc_error(payload: Dict[str, Any], code: int, message: str) -> Dict[str, Any]:
    msg_id = payload.get("id") if isinstance(payload, dict) else None
    return {"jsonrpc": "2.0", "id": msg_id, "error": {"code": code, "message": message}}


def _negotiate_version(requested: str | None) -> str:
    if requested in SUPPORTED_PROTOCOL_VERSIONS:
        return requested
    return SUPPORTED_PROTOCOL_VERSIONS[0]
