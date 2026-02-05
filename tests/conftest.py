from __future__ import annotations

import threading
from collections.abc import Callable
from pathlib import Path
from typing import List, Tuple

import pytest

from hoard.core.mcp.server import MCPRequestHandler, MCPServer


@pytest.fixture
def mcp_server() -> Callable[[Path], str]:
    servers: List[Tuple[MCPServer, threading.Thread]] = []

    def _start(config_path: Path) -> str:
        server = MCPServer(("127.0.0.1", 0), MCPRequestHandler, config_path)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        servers.append((server, thread))
        return f"http://127.0.0.1:{server.server_address[1]}/mcp"

    yield _start

    for server, thread in servers:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()
