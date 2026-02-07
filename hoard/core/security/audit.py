from __future__ import annotations

import json
from typing import Any, Dict, Optional

from hoard.core.security.limits import record_audit_event


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
    update_rate_limits: bool = True,
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
    if update_rate_limits:
        record_audit_event(
            token_name,
            tool,
            chunks_returned=chunks_returned,
            bytes_returned=bytes_returned,
        )
