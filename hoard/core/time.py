from __future__ import annotations

from datetime import UTC, datetime


def utc_now() -> datetime:
    return datetime.now(UTC)


def utc_now_naive() -> datetime:
    return utc_now().replace(tzinfo=None)


def utc_now_iso(*, timespec: str = "seconds") -> str:
    return utc_now().isoformat(timespec=timespec)


def utc_now_naive_iso(*, timespec: str = "seconds") -> str:
    return utc_now_naive().isoformat(timespec=timespec)
