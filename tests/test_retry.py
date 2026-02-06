from __future__ import annotations

import urllib.error

from hoard.sdk.retry import run_with_retry, should_retry_http_exception


def test_should_retry_http_exception_codes() -> None:
    retryable = urllib.error.HTTPError("https://x", 503, "Service Unavailable", hdrs=None, fp=None)
    non_retryable = urllib.error.HTTPError("https://x", 401, "Unauthorized", hdrs=None, fp=None)

    assert should_retry_http_exception(retryable) is True
    assert should_retry_http_exception(non_retryable) is False
    assert should_retry_http_exception(urllib.error.URLError("timeout")) is True


def test_run_with_retry_retries_transient_failures(monkeypatch) -> None:
    attempts = {"count": 0}
    monkeypatch.setattr("hoard.sdk.retry.time.sleep", lambda _: None)

    def flaky() -> str:
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise urllib.error.URLError("temporary")
        return "ok"

    result = run_with_retry(flaky, attempts=3)
    assert result == "ok"
    assert attempts["count"] == 3


def test_run_with_retry_stops_on_non_retryable() -> None:
    attempts = {"count": 0}

    def fail_fast() -> None:
        attempts["count"] += 1
        raise ValueError("bad request")

    try:
        run_with_retry(fail_fast, attempts=3)
    except ValueError:
        pass
    else:  # pragma: no cover
        raise AssertionError("Expected ValueError")

    assert attempts["count"] == 1
