from __future__ import annotations


def sanitize_fts_query(query: str) -> str:
    """Escape FTS5 metacharacters by wrapping each token in double quotes."""
    tokens = query.split()
    if not tokens:
        return query
    return " ".join('"' + token.replace('"', '""') + '"' for token in tokens)
