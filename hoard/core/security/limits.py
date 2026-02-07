from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Optional

from hoard.core.errors import HoardError


class RateLimitError(HoardError):
    pass


@dataclass
class _QuotaState:
    chunks_total: int = 0
    bytes_total: int = 0


class _InMemoryRateStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._request_events: dict[tuple[str, str], deque[float]] = defaultdict(deque)
        self._quota_events: dict[str, deque[tuple[float, int, int]]] = defaultdict(deque)
        self._quota_totals: dict[str, _QuotaState] = defaultdict(_QuotaState)

    def count_recent_requests(self, token_name: str, tool: str, window_seconds: int) -> int:
        now = time.time()
        cutoff = now - window_seconds
        key = (token_name, tool)
        with self._lock:
            events = self._request_events[key]
            while events and events[0] < cutoff:
                events.popleft()
            return len(events)

    def record_request(self, token_name: str, tool: str) -> None:
        now = time.time()
        with self._lock:
            self._request_events[(token_name, tool)].append(now)

    def get_quota_usage(self, token_name: str, window_seconds: int) -> tuple[int, int]:
        now = time.time()
        cutoff = now - window_seconds
        with self._lock:
            events = self._quota_events[token_name]
            totals = self._quota_totals[token_name]
            while events and events[0][0] < cutoff:
                _, chunks, bytes_count = events.popleft()
                totals.chunks_total = max(0, totals.chunks_total - chunks)
                totals.bytes_total = max(0, totals.bytes_total - bytes_count)
            return totals.chunks_total, totals.bytes_total

    def record_quota(self, token_name: str, chunks: int, bytes_returned: int) -> None:
        now = time.time()
        with self._lock:
            self._quota_events[token_name].append((now, chunks, bytes_returned))
            totals = self._quota_totals[token_name]
            totals.chunks_total += chunks
            totals.bytes_total += bytes_returned


_RATE_STORE = _InMemoryRateStore()


def record_audit_event(
    token_name: Optional[str],
    tool: str,
    *,
    chunks_returned: int = 0,
    bytes_returned: int = 0,
) -> None:
    """Compatibility hook used by synchronous audit writes."""
    if not token_name:
        return
    _RATE_STORE.record_request(token_name, tool)
    _RATE_STORE.record_quota(token_name, chunks_returned, bytes_returned)


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

        count = _RATE_STORE.count_recent_requests(token_name, tool, window_seconds=60)
        if count >= limit:
            raise RateLimitError(f"Rate limit exceeded for {tool}")

    def check_quota(self, token_name: Optional[str], chunks: int, bytes_returned: int) -> None:
        if not self.enforce or not token_name:
            return

        chunk_limit = int(self.limits.get("chunks_returned_per_hour", 0) or 0)
        byte_limit = int(self.limits.get("bytes_returned_per_hour", 0) or 0)

        used_chunks, used_bytes = _RATE_STORE.get_quota_usage(token_name, window_seconds=3600)

        if chunk_limit > 0 and used_chunks + chunks > chunk_limit:
            raise RateLimitError("Chunk quota exceeded")
        if byte_limit > 0 and used_bytes + bytes_returned > byte_limit:
            raise RateLimitError("Byte quota exceeded")

    def record_success(self, token_name: Optional[str], tool: str, chunks: int, bytes_returned: int) -> None:
        record_audit_event(
            token_name,
            tool,
            chunks_returned=chunks,
            bytes_returned=bytes_returned,
        )

    def record_failure(self, token_name: Optional[str], tool: str) -> None:
        record_audit_event(token_name, tool)

    def _limit_key_for_tool(self, tool: str) -> Optional[str]:
        if tool in {"search", "data.search"}:
            return "search_requests_per_minute"
        if tool in {
            "get",
            "get_chunk",
            "data.get",
            "data.get_chunk",
            "memory_get",
            "memory_put",
            "memory_search",
            "memory_write",
            "memory_query",
            "memory_retract",
            "memory_supersede",
            "memory_propose",
            "memory_review",
            "conflicts_list",
            "conflict_resolve",
            "duplicates_list",
            "duplicate_resolve",
            "sync_status",
            "sync_run",
            "sync",
            "inbox_put",
            "embeddings_build",
            "agent_register",
            "agent_list",
            "agent_remove",
            "memory.get",
            "memory.put",
            "memory.search",
            "memory.write",
            "memory.query",
            "memory.retract",
            "memory.supersede",
            "memory.propose",
            "memory.review",
            "memory.conflicts.list",
            "memory.conflicts.resolve",
            "memory.duplicates.list",
            "memory.duplicates.resolve",
            "ingest.sync",
            "ingest.status",
            "ingest.run",
            "ingest.embeddings.build",
            "ingest.inbox.put",
            "admin.agent.register",
            "admin.agent.list",
            "admin.agent.remove",
        }:
            return "get_requests_per_minute"
        return None
