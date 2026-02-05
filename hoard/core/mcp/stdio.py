from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import click

from hoard import __version__
from hoard.core.config import load_config, resolve_paths
from hoard.core.db.connection import connect, ensure_sqlite_version
from hoard.migrations import migrate
from hoard.core.mcp.tools import count_chunks, dispatch_tool, is_write_tool, tool_definitions
from hoard.core.security.auth import authenticate_token
from hoard.core.security.audit import log_access
from hoard.core.security.errors import AuthError, ScopeError
from hoard.core.security.limits import RateLimitError, RateLimiter

SUPPORTED_PROTOCOL_VERSIONS = ["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"]


@dataclass
class StdioState:
    initialized: bool = False
    protocol_version: str = SUPPORTED_PROTOCOL_VERSIONS[0]


class StdioMCPServer:
    def __init__(self, config_path: Path | None = None) -> None:
        self.config = load_config(config_path)
        paths = resolve_paths(self.config, config_path)
        self.db_path = paths.db_path
        self.state = StdioState()

    def serve_forever(self) -> None:
        stdin = sys.stdin.buffer
        for raw_line in stdin:
            line = raw_line.strip()
            if not line:
                continue

            try:
                message = json.loads(line.decode("utf-8"))
            except json.JSONDecodeError:
                self._write(self._build_error(None, -32700, "Parse error"))
                continue

            self._handle_message(message)

    def _handle_message(self, message: Dict[str, Any]) -> None:
        if isinstance(message, list):
            responses = []
            for item in message:
                response = self._handle_single_message(item)
                if response is not None:
                    responses.append(response)
            if responses:
                self._write(responses)
            return

        response = self._handle_single_message(message)
        if response is not None:
            self._write(response)

    def _handle_single_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(message, dict):
            return self._build_error(None, -32600, "Invalid Request")

        method = message.get("method")
        msg_id = message.get("id")
        params = message.get("params") or {}

        if not method:
            return self._build_error(msg_id, -32600, "Invalid Request")

        if method == "notifications/initialized":
            self.state.initialized = True
            return None

        if method == "initialize":
            return self._handle_initialize(msg_id, params)

        if not self.state.initialized:
            return self._build_error(msg_id, -32000, "Server not initialized")

        if method == "ping":
            return self._build_result(msg_id, {})

        if method == "tools/list":
            return self._build_result(
                msg_id,
                {"tools": tool_definitions(), "nextCursor": None},
            )

        if method == "tools/call":
            return self._handle_tools_call(msg_id, params)

        if msg_id is not None:
            return self._build_error(msg_id, -32601, "Method not found")
        return None

    def _handle_initialize(self, msg_id: Any, params: Dict[str, Any]) -> Dict[str, Any]:
        requested_version = params.get("protocolVersion")
        protocol_version = _negotiate_version(requested_version)
        self.state.protocol_version = protocol_version

        result = {
            "protocolVersion": protocol_version,
            "capabilities": {
                "tools": {"listChanged": False},
            },
            "serverInfo": {
                "name": "hoard",
                "version": __version__,
            },
            "instructions": "Provide auth token via tool arguments (token) or HOARD_TOKEN env.",
        }
        return self._build_result(msg_id, result)

    def _handle_tools_call(self, msg_id: Any, params: Dict[str, Any]) -> Dict[str, Any]:
        tool_name = params.get("name")
        arguments = params.get("arguments") or {}
        if not tool_name:
            return self._build_error(msg_id, -32602, "Missing tool name")

        token_value = os.getenv("HOARD_TOKEN") or arguments.get("token")
        if not token_value:
            return self._build_error(msg_id, -32000, "Missing auth token")

        conn = connect(self.db_path)
        limiter = RateLimiter(conn, self.config, enforce=False)

        try:
            token = authenticate_token(token_value, self.config)
            if is_write_tool(tool_name):
                return self._build_error(
                    msg_id,
                    -32004,
                    "Write tools require hoard serve (HTTP).",
                )
            limiter.check_request(token.name, tool_name)
            response = dispatch_tool(tool_name, arguments, conn, self.config, token)
            response_bytes = json.dumps(response).encode("utf-8")
            limiter.check_quota(token.name, count_chunks(response), len(response_bytes))
            content_result = {"content": [{"type": "text", "text": json.dumps(response)}]}
            return self._build_result(msg_id, content_result)
        except AuthError as exc:
            return self._build_error(msg_id, -32001, str(exc))
        except ScopeError as exc:
            return self._build_error(msg_id, -32002, str(exc))
        except RateLimitError as exc:
            return self._build_error(msg_id, -32003, str(exc))
        except ValueError as exc:
            log_access(conn, tool=tool_name, success=False, token_name=token.name)
            return self._build_error(msg_id, -32601, str(exc))
        except Exception as exc:
            return self._build_error(msg_id, -32603, str(exc))
        finally:
            conn.close()

    def _build_result(self, msg_id: Any, result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if msg_id is None:
            return None
        return {"jsonrpc": "2.0", "id": msg_id, "result": result}

    def _build_error(self, msg_id: Any, code: int, message: str) -> Dict[str, Any]:
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "error": {"code": code, "message": message},
        }

    def _write(self, payload: Dict[str, Any]) -> None:
        data = json.dumps(payload).encode("utf-8")
        sys.stdout.buffer.write(data + b"\n")
        sys.stdout.buffer.flush()


def run_stdio(config_path: Path | None = None) -> None:
    server = StdioMCPServer(config_path)

    conn = connect(server.db_path)
    try:
        ensure_sqlite_version()
        migrate(conn, app_version=__version__)
    except Exception as exc:
        click.echo(f"Failed to apply migrations: {exc}", err=True)
        raise
    finally:
        conn.close()

    server.serve_forever()


def _negotiate_version(requested: Optional[str]) -> str:
    if requested in SUPPORTED_PROTOCOL_VERSIONS:
        return requested
    return SUPPORTED_PROTOCOL_VERSIONS[0]
