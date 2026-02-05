from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Iterable, List, Optional, Set

from hoard.core.security.agent_tokens import authenticate_agent
from hoard.core.security.errors import AuthError, ScopeError


@dataclass(frozen=True)
class TokenInfo:
    name: str
    token: Optional[str]
    scopes: Set[str]
    capabilities: Set[str]
    trust_level: float
    can_access_sensitive: bool
    can_access_restricted: bool
    requires_user_confirm: bool
    proposal_ttl_days: Optional[int]
    rate_limit_per_hour: int


def load_tokens(config: dict) -> List[TokenInfo]:
    tokens = []
    raw_tokens = config.get("security", {}).get("tokens", [])
    for item in raw_tokens:
        name = item.get("name")
        token = item.get("token")
        scopes = set(item.get("scopes", []))
        if not name or not token:
            continue
        tokens.append(
            TokenInfo(
                name=name,
                token=token,
                scopes=scopes,
                capabilities=scopes,
                trust_level=0.5,
                can_access_sensitive="sensitive" in scopes,
                can_access_restricted="restricted" in scopes,
                requires_user_confirm=False,
                proposal_ttl_days=None,
                rate_limit_per_hour=0,
            )
        )
    return tokens


def authenticate_token(token_value: str, config: dict, conn=None) -> TokenInfo:
    if conn is not None and _server_secret_available(config):
        agent = authenticate_agent(conn, token_value, config)
        return TokenInfo(
            name=agent.agent_id,
            token=None,
            scopes=agent.scopes,
            capabilities=agent.capabilities,
            trust_level=agent.trust_level,
            can_access_sensitive=agent.can_access_sensitive,
            can_access_restricted=agent.can_access_restricted,
            requires_user_confirm=agent.requires_user_confirm,
            proposal_ttl_days=agent.proposal_ttl_days,
            rate_limit_per_hour=agent.rate_limit_per_hour,
        )

    for token in load_tokens(config):
        if token.token == token_value:
            return token
    raise AuthError("Invalid token")


def _server_secret_available(config: dict) -> bool:
    env_key = config.get("write", {}).get("server_secret_env", "HOARD_SERVER_SECRET")
    return bool(env_key and os.environ.get(env_key))


def require_scopes(token: TokenInfo, required: Iterable[str]) -> None:
    missing = [scope for scope in required if scope not in token.scopes]
    if missing:
        raise ScopeError(f"Missing scopes: {', '.join(missing)}")


def can_access_sensitive(token: TokenInfo | None) -> bool:
    if token is None:
        return True
    return bool(token.can_access_sensitive)
