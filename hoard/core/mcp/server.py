from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict

import click

from hoard import __version__
from hoard.core.config import load_config, resolve_paths
from hoard.core.db.connection import connect, ensure_sqlite_version
from hoard.core.db.writer import WriteCoordinator
from hoard.core.mcp.tools import count_chunks, dispatch_tool, is_write_tool, tool_definitions
from hoard.core.security.agent_tokens import ensure_agent_from_config
from hoard.migrations import get_current_version, get_migrations, migrate
from hoard.core.security.audit import log_access
from hoard.core.security.auth import TokenInfo, authenticate_token
from hoard.core.security.errors import AuthError, ScopeError
from hoard.core.security.limits import RateLimitError, RateLimiter
from hoard.core.worker import Worker

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
            token = self._authenticate(conn)
            limiter.check_request(token.name, tool)
            response = self._dispatch_tool(tool, payload, conn, token)
            response_bytes = json.dumps(response).encode("utf-8")
            limiter.check_quota(token.name, count_chunks(response), len(response_bytes))
            self._log_access(
                tool=tool,
                success=True,
                token_name=token.name,
                scope=None,
                chunks_returned=count_chunks(response),
                bytes_returned=len(response_bytes),
            )
            self._write_json(200, response)
        except AuthError as exc:
            self._log_access(tool=tool, success=False, token_name=None)
            self._write_json(401, {"error": str(exc)})
        except ScopeError as exc:
            self._log_access(tool=tool, success=False, token_name=getattr(token, "name", None))
            self._write_json(403, {"error": str(exc)})
        except RateLimitError as exc:
            self._log_access(tool=tool, success=False, token_name=getattr(token, "name", None))
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
            token = self._authenticate(conn)
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
            if not tool_name:
                return _jsonrpc_error(payload, -32602, "Missing tool name")
            try:
                limiter.check_request(token.name, tool_name)
                response = self._dispatch_tool(tool_name, arguments, conn, token)
                response_bytes = json.dumps(response).encode("utf-8")
                limiter.check_quota(token.name, count_chunks(response), len(response_bytes))
                self._log_access(
                    tool=tool_name,
                    success=True,
                    token_name=token.name,
                    chunks_returned=count_chunks(response),
                    bytes_returned=len(response_bytes),
                )
                return {"jsonrpc": "2.0", "id": msg_id, "result": response}
            except ValueError as exc:
                self._log_access(
                    tool=tool_name,
                    success=False,
                    token_name=token.name,
                )
                return _jsonrpc_error(payload, -32601, str(exc))
            except Exception as exc:  # pragma: no cover - safety net
                self._log_access(
                    tool=tool_name,
                    success=False,
                    token_name=token.name,
                )
                return _jsonrpc_error(payload, -32603, str(exc))

        return _jsonrpc_error(payload, -32601, "Method not found")

    def _tool_name(self) -> str:
        return self.path.lstrip("/")

    def _authenticate(self, conn):
        header = self.headers.get("Authorization", "")
        if not header.startswith("Bearer "):
            raise AuthError("Missing bearer token")
        token_value = header.split(" ", 1)[1]
        env_key = self.server.config.get("write", {}).get("server_secret_env", "HOARD_SERVER_SECRET")
        server_secret = os.environ.get(env_key)
        if server_secret and token_value == server_secret:
            return TokenInfo(
                name="admin",
                token=None,
                scopes={"admin", "sync", "memory", "search", "get", "ingest"},
                capabilities={"admin", "sync", "memory", "search", "get", "ingest"},
                trust_level=1.0,
                can_access_sensitive=True,
                can_access_restricted=True,
                requires_user_confirm=False,
                proposal_ttl_days=None,
                rate_limit_per_hour=0,
            )
        return authenticate_token(token_value, self.server.config, conn)

    def _dispatch_tool(self, tool: str, payload: Dict[str, Any], conn, token):
        if is_write_tool(tool):
            return self.server.writer.submit(
                lambda writer_conn: dispatch_tool(tool, payload, writer_conn, self.server.config, token)
            )
        return dispatch_tool(tool, payload, conn, self.server.config, token)

    def _log_access(
        self,
        *,
        tool: str,
        success: bool,
        token_name: str | None,
        scope: str | None = None,
        chunks_returned: int = 0,
        bytes_returned: int = 0,
    ) -> None:
        self.server.writer.submit(
            log_access,
            tool=tool,
            success=success,
            token_name=token_name,
            scope=scope,
            chunks_returned=chunks_returned,
            bytes_returned=bytes_returned,
        )

    def _write_json(self, status: int, payload: Dict[str, Any]) -> None:
        response = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


class MCPServer(ThreadingHTTPServer):
    def __init__(self, server_address, RequestHandlerClass, config_path: Path | None) -> None:
        self.config = load_config(config_path)
        paths = resolve_paths(self.config, config_path)
        self.db_path = paths.db_path
        busy_timeout = int(self.config.get("write", {}).get("database", {}).get("busy_timeout_ms", 0) or 0)
        self.writer = WriteCoordinator(db_path=self.db_path, busy_timeout_ms=busy_timeout or None)
        self.worker = Worker(db_path=self.db_path, config=self.config, writer=self.writer)
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
        ensure_sqlite_version()
        _require_server_secret(server.config)
        if no_migrate:
            migrations = get_migrations()
            latest = max(migrations.keys()) if migrations else 0
            current = get_current_version(conn)
            if current < latest:
                click.echo(f"⚠️  Schema migrations pending (v{current} → v{latest})")
                click.echo("   Run 'hoard db migrate' or restart without --no-migrate")
        else:
            migrate(conn, app_version=__version__)
        _bootstrap_tokens(conn, server.config)
    finally:
        conn.close()

    server.worker.start()
    server.serve_forever()


def _jsonrpc_error(payload: Dict[str, Any], code: int, message: str) -> Dict[str, Any]:
    msg_id = payload.get("id") if isinstance(payload, dict) else None
    return {"jsonrpc": "2.0", "id": msg_id, "error": {"code": code, "message": message}}


def _negotiate_version(requested: str | None) -> str:
    if requested in SUPPORTED_PROTOCOL_VERSIONS:
        return requested
    return SUPPORTED_PROTOCOL_VERSIONS[0]


def _require_server_secret(config: dict) -> None:
    if not config.get("write", {}).get("enabled", True):
        return
    env_key = config.get("write", {}).get("server_secret_env", "HOARD_SERVER_SECRET")
    if not env_key:
        raise RuntimeError("write.server_secret_env is not configured")
    if not os.environ.get(env_key):
        raise RuntimeError(f"{env_key} environment variable not set")


def _bootstrap_tokens(conn, config: dict) -> None:
    tokens = config.get("security", {}).get("tokens", [])
    for token in tokens:
        name = token.get("name")
        value = token.get("token")
        scopes = token.get("scopes", [])
        if not name or not value:
            continue
        ensure_agent_from_config(conn, config, name, value, scopes)
    if tokens:
        conn.commit()
