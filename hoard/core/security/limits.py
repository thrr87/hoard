from __future__ import annotations

from typing import Optional


class RateLimitError(Exception):
    pass


class RateLimiter:
    def __init__(self, conn, config: dict, enforce: bool = True) -> None:
        self.conn = conn
        self.enforce = enforce
        self.limits = config.get("security", {}).get("rate_limits", {})

    def check_request(self, token_name: Optional[str], tool: str) -> None:
        if not self.enforce or not token_name:
            return

        limit_key = self._limit_key_for_tool(tool)
        if not limit_key:
            return

        limit = int(self.limits.get(limit_key, 0) or 0)
        if limit <= 0:
            return

        count = self._count_requests(token_name, tool)
        if count >= limit:
            raise RateLimitError(f"Rate limit exceeded for {tool}")

    def check_quota(self, token_name: Optional[str], chunks: int, bytes_returned: int) -> None:
        if not self.enforce or not token_name:
            return

        chunk_limit = int(self.limits.get("chunks_returned_per_hour", 0) or 0)
        byte_limit = int(self.limits.get("bytes_returned_per_hour", 0) or 0)

        if chunk_limit > 0:
            used_chunks = self._sum_metric(token_name, "chunks_returned")
            if used_chunks + chunks > chunk_limit:
                raise RateLimitError("Chunk quota exceeded")

        if byte_limit > 0:
            used_bytes = self._sum_metric(token_name, "bytes_returned")
            if used_bytes + bytes_returned > byte_limit:
                raise RateLimitError("Byte quota exceeded")

    def _limit_key_for_tool(self, tool: str) -> Optional[str]:
        if tool in {"search"}:
            return "search_requests_per_minute"
        if tool in {"get", "get_chunk", "memory_get", "memory_put", "memory_search", "sync_status"}:
            return "get_requests_per_minute"
        return None

    def _count_requests(self, token_name: str, tool: str) -> int:
        row = self.conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM audit_logs
            WHERE token_name = ?
              AND tool = ?
              AND request_at >= datetime('now', '-1 minute')
            """,
            (token_name, tool),
        ).fetchone()
        return int(row[0]) if row else 0

    def _sum_metric(self, token_name: str, metric: str) -> int:
        row = self.conn.execute(
            f"""
            SELECT COALESCE(SUM({metric}), 0) AS total
            FROM audit_logs
            WHERE token_name = ?
              AND request_at >= datetime('now', '-1 hour')
            """,
            (token_name,),
        ).fetchone()
        return int(row[0]) if row else 0
