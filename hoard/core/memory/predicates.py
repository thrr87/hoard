from __future__ import annotations

from typing import List, Tuple


def active_memory_conditions(now_iso: str, table_alias: str = "m") -> tuple[List[str], List[str]]:
    conditions = [
        f"{table_alias}.retracted_at IS NULL",
        f"{table_alias}.superseded_at IS NULL",
        f"({table_alias}.expires_at IS NULL OR {table_alias}.expires_at > ?)",
    ]
    params = [now_iso]
    return conditions, params


def inactive_memory_conditions(now_iso: str, table_alias: str = "m") -> tuple[List[str], List[str]]:
    conditions = [
        f"({table_alias}.retracted_at IS NOT NULL",
        f"OR {table_alias}.superseded_at IS NOT NULL",
        f"OR ({table_alias}.expires_at IS NOT NULL AND {table_alias}.expires_at <= ?))",
    ]
    params = [now_iso]
    return [" ".join(conditions)], params
