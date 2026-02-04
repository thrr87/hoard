from __future__ import annotations

import hmac
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Optional

from argon2 import PasswordHasher, Type

from hoard.core.security.errors import AuthError


@dataclass(frozen=True)
class AgentInfo:
    agent_id: str
    scopes: set[str]
    capabilities: set[str]
    trust_level: float
    can_access_sensitive: bool
    can_access_restricted: bool
    requires_user_confirm: bool
    proposal_ttl_days: Optional[int]
    rate_limit_per_hour: int


def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def _server_secret(config: dict) -> bytes:
    env_key = config.get("write", {}).get("server_secret_env", "HOARD_SERVER_SECRET")
    secret = os.environ.get(env_key)
    if not secret:
        raise RuntimeError(f"{env_key} environment variable not set")
    return secret.encode()


def _hasher() -> PasswordHasher:
    return PasswordHasher(type=Type.ID, time_cost=2, memory_cost=65536, parallelism=1)


def compute_lookup_hash(token: str, config: dict) -> str:
    secret = _server_secret(config)
    return hmac.new(secret, token.encode(), hashlib.sha256).hexdigest()


def compute_secure_hash(token: str) -> str:
    return _hasher().hash(token)


def verify_secure_hash(token: str, hashed: str) -> bool:
    try:
        return _hasher().verify(hashed, token)
    except Exception:
        return False


def register_agent(
    conn,
    *,
    config: dict,
    agent_id: str,
    token: str,
    scopes: Iterable[str],
    capabilities: Optional[Iterable[str]] = None,
    trust_level: float = 0.5,
    requires_user_confirm: bool = False,
    proposal_ttl_days: Optional[int] = None,
    rate_limit_per_hour: int = 100,
    overwrite: bool = False,
) -> None:
    scope_list = sorted({s for s in scopes if s})
    capability_list = sorted({s for s in (capabilities or scope_list) if s})
    lookup_hash = compute_lookup_hash(token, config)
    secure_hash = compute_secure_hash(token)

    can_access_sensitive = 1 if "sensitive" in scope_list else 0
    can_access_restricted = 1 if "restricted" in scope_list else 0

    existing = conn.execute(
        "SELECT agent_id FROM agent_tokens WHERE agent_id = ?",
        (agent_id,),
    ).fetchone()
    if existing and not overwrite:
        raise AuthError(f"Agent {agent_id} already exists")
    if existing and overwrite:
        conn.execute(
            """
            UPDATE agent_tokens
            SET token_lookup_hash = ?, token_secure_hash = ?, trust_level = ?,
                capabilities = ?, allowed_scopes = ?, rate_limit_per_hour = ?,
                requires_user_confirm = ?, proposal_ttl_days = ?,
                can_access_sensitive = ?, can_access_restricted = ?, last_used_at = NULL
            WHERE agent_id = ?
            """,
            (
                lookup_hash,
                secure_hash,
                trust_level,
                json.dumps(capability_list),
                json.dumps(scope_list),
                rate_limit_per_hour,
                1 if requires_user_confirm else 0,
                proposal_ttl_days,
                can_access_sensitive,
                can_access_restricted,
                agent_id,
            ),
        )
        return

    conn.execute(
        """
        INSERT INTO agent_tokens (
            agent_id,
            token_lookup_hash,
            token_secure_hash,
            trust_level,
            capabilities,
            allowed_scopes,
            rate_limit_per_hour,
            requires_user_confirm,
            proposal_ttl_days,
            can_access_sensitive,
            can_access_restricted,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            agent_id,
            lookup_hash,
            secure_hash,
            trust_level,
            json.dumps(capability_list),
            json.dumps(scope_list),
            rate_limit_per_hour,
            1 if requires_user_confirm else 0,
            proposal_ttl_days,
            can_access_sensitive,
            can_access_restricted,
            _now_iso(),
        ),
    )


def ensure_agent_from_config(conn, config: dict, name: str, token: str, scopes: Iterable[str]) -> None:
    if not name or not token:
        return
    lookup_hash = compute_lookup_hash(token, config)
    existing = conn.execute(
        "SELECT agent_id FROM agent_tokens WHERE token_lookup_hash = ?",
        (lookup_hash,),
    ).fetchone()
    if existing:
        return

    register_agent(
        conn,
        config=config,
        agent_id=name,
        token=token,
        scopes=scopes,
        capabilities=scopes,
        rate_limit_per_hour=int(
            config.get("write", {})
            .get("limits", {})
            .get("per_agent", {})
            .get("max_writes_per_hour", 100)
        ),
    )


def authenticate_agent(conn, token: str, config: dict) -> AgentInfo:
    if not token:
        raise AuthError("Missing token")
    lookup_hash = compute_lookup_hash(token, config)
    row = conn.execute(
        "SELECT * FROM agent_tokens WHERE token_lookup_hash = ?",
        (lookup_hash,),
    ).fetchone()
    if not row:
        raise AuthError("Invalid token")

    scopes = set(json.loads(row["allowed_scopes"])) if row["allowed_scopes"] else set()
    capabilities = set(json.loads(row["capabilities"])) if row["capabilities"] else set()

    return AgentInfo(
        agent_id=row["agent_id"],
        scopes=scopes,
        capabilities=capabilities,
        trust_level=float(row["trust_level"]),
        can_access_sensitive=bool(row["can_access_sensitive"]),
        can_access_restricted=bool(row["can_access_restricted"]),
        requires_user_confirm=bool(row["requires_user_confirm"]),
        proposal_ttl_days=row["proposal_ttl_days"],
        rate_limit_per_hour=int(row["rate_limit_per_hour"] or 0),
    )


def list_agents(conn) -> List[AgentInfo]:
    rows = conn.execute("SELECT * FROM agent_tokens ORDER BY agent_id").fetchall()
    agents: List[AgentInfo] = []
    for row in rows:
        scopes = set(json.loads(row["allowed_scopes"])) if row["allowed_scopes"] else set()
        capabilities = set(json.loads(row["capabilities"])) if row["capabilities"] else set()
        agents.append(
            AgentInfo(
                agent_id=row["agent_id"],
                scopes=scopes,
                capabilities=capabilities,
                trust_level=float(row["trust_level"]),
                can_access_sensitive=bool(row["can_access_sensitive"]),
                can_access_restricted=bool(row["can_access_restricted"]),
                requires_user_confirm=bool(row["requires_user_confirm"]),
                proposal_ttl_days=row["proposal_ttl_days"],
                rate_limit_per_hour=int(row["rate_limit_per_hour"] or 0),
            )
        )
    return agents


def delete_agent(conn, agent_id: str) -> bool:
    cursor = conn.execute("DELETE FROM agent_tokens WHERE agent_id = ?", (agent_id,))
    return cursor.rowcount > 0
