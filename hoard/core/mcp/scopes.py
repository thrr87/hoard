from __future__ import annotations

from typing import Iterable

from hoard.core.security.errors import ScopeError


def has_any_scope(token, scopes: Iterable[str]) -> bool:
    token_scopes = getattr(token, "scopes", set())
    if "admin" in token_scopes:
        return True
    return any(scope in token_scopes for scope in scopes)


def require_any_scope(token, scopes: Iterable[str], *, message: str | None = None) -> None:
    scopes_set = set(scopes)
    if has_any_scope(token, scopes_set):
        return
    if message:
        raise ScopeError(message)
    raise ScopeError(f"Missing scopes: {', '.join(sorted(scopes_set))}")


def require_scope(token, scope: str, *, message: str | None = None) -> None:
    require_any_scope(token, {scope}, message=message)

