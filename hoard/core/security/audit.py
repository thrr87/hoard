from __future__ import annotations

import json
from typing import Any, Dict, Optional


def log_access(
    conn,
    *,
    tool: str,
    success: bool,
    token_name: str | None = None,
    scope: str | None = None,
    chunks_returned: int = 0,
    bytes_returned: int = 0,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    payload = json.dumps(metadata) if metadata else None
    conn.execute(
        """
        INSERT INTO audit_logs (
            token_name, tool, scope, success, chunks_returned, bytes_returned, metadata
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            token_name,
            tool,
            scope,
            1 if success else 0,
            chunks_returned,
            bytes_returned,
            payload,
        ),
    )
    conn.commit()
