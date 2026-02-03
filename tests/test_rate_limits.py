from __future__ import annotations

import pytest
from pathlib import Path

from hoard.core.db.connection import connect, initialize_db
from hoard.core.security.audit import log_access
from hoard.core.security.limits import RateLimitError, RateLimiter


def test_rate_limit_requests(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    config = {
        "security": {
            "rate_limits": {
                "search_requests_per_minute": 2,
                "get_requests_per_minute": 1,
                "chunks_returned_per_hour": 100,
                "bytes_returned_per_hour": 1000,
            }
        }
    }

    limiter = RateLimiter(conn, config, enforce=True)
    log_access(conn, tool="search", success=True, token_name="tester")
    log_access(conn, tool="search", success=True, token_name="tester")

    with pytest.raises(RateLimitError):
        limiter.check_request("tester", "search")

    conn.close()


def test_rate_limit_quota(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    config = {
        "security": {
            "rate_limits": {
                "search_requests_per_minute": 10,
                "get_requests_per_minute": 10,
                "chunks_returned_per_hour": 2,
                "bytes_returned_per_hour": 10,
            }
        }
    }

    limiter = RateLimiter(conn, config, enforce=True)
    log_access(conn, tool="search", success=True, token_name="tester", chunks_returned=2, bytes_returned=5)

    with pytest.raises(RateLimitError):
        limiter.check_quota("tester", chunks=1, bytes_returned=1)

    with pytest.raises(RateLimitError):
        limiter.check_quota("tester", chunks=0, bytes_returned=10)

    conn.close()
