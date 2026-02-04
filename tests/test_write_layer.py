from __future__ import annotations

import os
from pathlib import Path

import pytest

from hoard.core.db.connection import connect, ensure_sqlite_version, initialize_db
from hoard.core.memory.embeddings import decode_embedding, encode_embedding, validate_embedding_blob
from hoard.core.memory.predicates import active_memory_conditions
from hoard.core.memory.v2.store import memory_query, memory_write
from hoard.core.security.agent_tokens import compute_lookup_hash, compute_secure_hash, verify_secure_hash
from hoard.core.security.auth import TokenInfo


def _test_config() -> dict:
    return {
        "write": {
            "slots": {
                "pattern": r"^(pref|fact|ctx|decision|event):[a-z0-9_]+(\.[a-z0-9_]+){0,3}$",
                "on_invalid": "reject",
            },
            "query": {
                "hybrid_weight_vector": 0.6,
                "hybrid_weight_fts": 0.4,
                "slot_match_bonus": 0.1,
                "slot_only_baseline": 0.5,
                "union_multiplier": 2,
            },
            "embeddings": {"enabled": False, "active_model": {"name": "stub"}},
        },
        "vectors": {"enabled": False},
    }


def _agent() -> TokenInfo:
    return TokenInfo(
        name="tester",
        token=None,
        scopes={"memory"},
        capabilities={"memory"},
        trust_level=0.5,
        can_access_sensitive=True,
        can_access_restricted=True,
        requires_user_confirm=False,
        proposal_ttl_days=None,
        rate_limit_per_hour=0,
    )


def test_sqlite_version_check() -> None:
    ensure_sqlite_version((3, 0, 0))
    with pytest.raises(RuntimeError):
        ensure_sqlite_version((99, 0, 0))


def test_embedding_encode_decode_roundtrip() -> None:
    vector = [1.0, 0.0, 0.0, 0.0]
    blob = encode_embedding(vector, 4)
    assert validate_embedding_blob(blob, 4)
    decoded = decode_embedding(blob, 4)
    assert len(decoded) == 4
    assert abs(decoded[0] - 1.0) < 1e-5


def test_token_hashing() -> None:
    os.environ["HOARD_SERVER_SECRET"] = "test-secret"
    lookup = compute_lookup_hash("token-value", {"write": {"server_secret_env": "HOARD_SERVER_SECRET"}})
    assert lookup
    secure = compute_secure_hash("token-value")
    assert verify_secure_hash("token-value", secure)


def test_active_includes_expires() -> None:
    conditions, _ = active_memory_conditions("2026-01-01T00:00:00")
    assert any("expires_at" in cond for cond in conditions)


def test_memory_query_scores(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    config = _test_config()
    agent = _agent()

    memory_write(
        conn,
        content="Alpha memory",
        memory_type="context",
        scope_type="user",
        scope_id=None,
        source_agent="tester",
        tags=["Test"],
        config=config,
    )

    result = memory_query(conn, params={"query": "Alpha", "limit": 5}, agent=agent, config=config)
    results = result.get("results", [])
    assert results
    assert "score" in results[0]
    conn.close()


def test_tag_normalization(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    config = _test_config()
    memory = memory_write(
        conn,
        content="Tagged memory",
        memory_type="context",
        scope_type="user",
        scope_id=None,
        source_agent="tester",
        tags=["Foo", "bar"],
        config=config,
    )

    rows = conn.execute(
        "SELECT tag FROM memory_tags WHERE memory_id = ? ORDER BY tag",
        (memory["id"],),
    ).fetchall()
    assert [row[0] for row in rows] == ["bar", "foo"]
    conn.close()


def test_restricted_slot_query(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    config = _test_config()
    agent = _agent()

    memory = memory_write(
        conn,
        content="Restricted memory",
        memory_type="fact",
        scope_type="user",
        scope_id=None,
        source_agent="tester",
        slot="fact:restricted",
        sensitivity="restricted",
        config=config,
    )

    result = memory_query(
        conn,
        params={"query": "nothing", "slot": "fact:restricted", "limit": 5},
        agent=agent,
        config=config,
    )
    ids = [entry["id"] for entry in result.get("results", [])]
    assert memory["id"] in ids
    conn.close()


def test_slot_union_bounded(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    config = _test_config()
    agent = _agent()

    for idx in range(5):
        memory_write(
            conn,
            content=f"Slot memory {idx}",
            memory_type="context",
            scope_type="user",
            scope_id=None,
            source_agent="tester",
            slot="ctx:slot",
            config=config,
        )

    result = memory_query(
        conn,
        params={"query": "slot", "slot": "ctx:slot", "limit": 2},
        agent=agent,
        config=config,
    )
    assert len(result.get("results", [])) <= 2
    conn.close()
