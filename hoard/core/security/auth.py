from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Set


class AuthError(Exception):
    pass


class ScopeError(Exception):
    pass


@dataclass(frozen=True)
class TokenInfo:
    name: str
    token: str
    scopes: Set[str]


def load_tokens(config: dict) -> List[TokenInfo]:
    tokens = []
    raw_tokens = config.get("security", {}).get("tokens", [])
    for item in raw_tokens:
        name = item.get("name")
        token = item.get("token")
        scopes = set(item.get("scopes", []))
        if not name or not token:
            continue
        tokens.append(TokenInfo(name=name, token=token, scopes=scopes))
    return tokens


def authenticate_token(token_value: str, config: dict) -> TokenInfo:
    for token in load_tokens(config):
        if token.token == token_value:
            return token
    raise AuthError("Invalid token")


def require_scopes(token: TokenInfo, required: Iterable[str]) -> None:
    missing = [scope for scope in required if scope not in token.scopes]
    if missing:
        raise ScopeError(f"Missing scopes: {', '.join(missing)}")


def can_access_sensitive(token: TokenInfo | None) -> bool:
    if token is None:
        return True
    return "sensitive" in token.scopes
