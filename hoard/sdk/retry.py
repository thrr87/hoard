from __future__ import annotations

import time
import urllib.error
from typing import Callable, TypeVar

T = TypeVar("T")
RETRYABLE_HTTP_CODES = {502, 503, 504}


def should_retry_http_exception(exc: Exception) -> bool:
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code in RETRYABLE_HTTP_CODES
    if isinstance(exc, urllib.error.URLError):
        return True
    if isinstance(exc, TimeoutError):
        return True
    return False


def run_with_retry(
    fn: Callable[[], T],
    *,
    attempts: int = 3,
    initial_delay_seconds: float = 0.25,
    backoff_multiplier: float = 2.0,
    should_retry: Callable[[Exception], bool] | None = None,
) -> T:
    if attempts < 1:
        raise ValueError("attempts must be >= 1")

    classifier = should_retry or should_retry_http_exception
    delay = initial_delay_seconds

    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:
            if attempt >= attempts or not classifier(exc):
                raise
            if delay > 0:
                time.sleep(delay)
                delay *= backoff_multiplier

    raise RuntimeError("Retry loop exited unexpectedly.")
